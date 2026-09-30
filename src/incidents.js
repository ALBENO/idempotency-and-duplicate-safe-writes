const crypto = require('crypto');
const { db } = require('./db');

const OPERATION = 'POST:/incidents';

function canonicalize(value) {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }

  if (value && typeof value === 'object') {
    return Object.keys(value)
      .sort()
      .reduce((result, key) => {
        result[key] = canonicalize(value[key]);
        return result;
      }, {});
  }

  return value;
}

function hashRequest(body) {
  const canonical = JSON.stringify(canonicalize(body));

  return crypto
    .createHash('sha256')
    .update(canonical)
    .digest('hex');
}

async function createIncident(req, res) {
  const key = req.get('Idempotency-Key');

  if (!key || !key.trim()) {
    return res.status(400).json({
      error: 'idempotency_key_required'
    });
  }

  const tenantId = req.user.tenantId;
  const requestHash = hashRequest(req.body);

  let result;
  let replayed = false;

  await db.tx(async (t) => {
    /*
     * First attempt to atomically claim the idempotency key.
     *
     * PostgreSQL's UNIQUE constraint makes concurrent requests
     * compete for the same logical key.
     */
    const claimed = await t.oneOrNone(
      `
      INSERT INTO idempotency_keys (
        tenant_id,
        operation,
        key,
        request_hash,
        state,
        expires_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        'processing',
        now() + interval '24 hours'
      )
      ON CONFLICT (tenant_id, operation, key)
      DO NOTHING
      RETURNING *
      `,
      [tenantId, OPERATION, key, requestHash]
    );

    /*
     * We successfully claimed the key.
     * This request is responsible for creating the incident.
     */
    if (claimed) {
      const { title, severity, serviceId } = req.body;

      const incident = await t.one(
        `
        INSERT INTO incidents (
          tenant_id,
          service_id,
          title,
          severity
        )
        VALUES ($1, $2, $3, $4)
        RETURNING *
        `,
        [tenantId, serviceId, title, severity]
      );

      /*
       * The paging job is created inside the same transaction.
       * Therefore incident + paging job either both commit
       * or both roll back.
       */
      await t.none(
        `
        INSERT INTO paging_jobs (
          incident_id,
          tenant_id,
          status
        )
        VALUES ($1, $2, 'pending')
        `,
        [incident.id, tenantId]
      );

      /*
       * Store the completed response so a retry can replay
       * exactly the same result.
       */
      await t.none(
        `
        UPDATE idempotency_keys
        SET
          state = 'completed',
          incident_id = $2,
          response_status = 201,
          response_body = $3::jsonb
        WHERE id = $1
        `,
        [
          claimed.id,
          incident.id,
          JSON.stringify(incident)
        ]
      );

      result = {
        status: 201,
        body: incident
      };

      return;
    }

    /*
     * Another request already owns this key.
     *
     * SELECT ... FOR UPDATE makes sure we see the committed
     * state and serialize access to the idempotency record.
     */
    let existing = await t.one(
      `
      SELECT *
      FROM idempotency_keys
      WHERE tenant_id = $1
        AND operation = $2
        AND key = $3
      FOR UPDATE
      `,
      [tenantId, OPERATION, key]
    );

    /*
     * Expired idempotency records can be reused.
     */
    if (existing.expires_at <= new Date()) {
      await t.none(
        `
        DELETE FROM idempotency_keys
        WHERE id = $1
        `,
        [existing.id]
      );

      const reclaimed = await t.one(
        `
        INSERT INTO idempotency_keys (
          tenant_id,
          operation,
          key,
          request_hash,
          state,
          expires_at
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          'processing',
          now() + interval '24 hours'
        )
        RETURNING *
        `,
        [tenantId, OPERATION, key, requestHash]
      );

      const { title, severity, serviceId } = req.body;

      const incident = await t.one(
        `
        INSERT INTO incidents (
          tenant_id,
          service_id,
          title,
          severity
        )
        VALUES ($1, $2, $3, $4)
        RETURNING *
        `,
        [tenantId, serviceId, title, severity]
      );

      await t.none(
        `
        INSERT INTO paging_jobs (
          incident_id,
          tenant_id,
          status
        )
        VALUES ($1, $2, 'pending')
        `,
        [incident.id, tenantId]
      );

      await t.none(
        `
        UPDATE idempotency_keys
        SET
          state = 'completed',
          incident_id = $2,
          response_status = 201,
          response_body = $3::jsonb
        WHERE id = $1
        `,
        [
          reclaimed.id,
          incident.id,
          JSON.stringify(incident)
        ]
      );

      result = {
        status: 201,
        body: incident
      };

      return;
    }

    /*
     * Same key but different request contents.
     */
    if (existing.request_hash !== requestHash) {
      result = {
        status: 409,
        body: {
          error: 'idempotency_key_conflict'
        }
      };

      return;
    }

    /*
     * Same key + same request, but another operation is
     * currently executing.
     */
    if (existing.state === 'processing') {
      result = {
        status: 409,
        body: {
          error: 'operation_in_progress'
        }
      };

      return;
    }

    /*
     * Previous attempt failed.
     */
    if (existing.state === 'failed') {
      result = {
        status: 409,
        body: {
          error: 'prior_operation_failed'
        }
      };

      return;
    }

    /*
     * Completed request: replay the stored response.
     */
    if (existing.state === 'completed') {
      replayed = true;

      result = {
        status: existing.response_status,
        body: existing.response_body
      };

      return;
    }
  });

  if (replayed) {
    res.set('Idempotent-Replayed', 'true');
  }

  return res.status(result.status).json(result.body);
}

module.exports = {
  createIncident,
  hashRequest
};
