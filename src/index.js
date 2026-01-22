const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const pool = new Pool({
  host: process.env.POSTGRES_HOST,   
  port: process.env.POSTGRES_PORT,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});


/**
 * Products borrowed more than 3 months ago
 */
app.get('/product-lost', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT *
      FROM (
        SELECT DISTINCT ON (transaction_id) *
        FROM product_events
        ORDER BY transaction_id, evt_date DESC
      ) t
      WHERE t.evt_type = 'borrow'
        AND t.evt_date < NOW() - INTERVAL '3 months'
    `);

    res.status(200).json({
      status: 'success',
      message: 'Successfully retrieved lost products',
      data: rows,
    });
  } catch (err) {
    console.error('[PRODUCT_LOST_ERROR]', err);

    res.status(500).json({
      status: 'error',
      message: 'Failed to retrieve lost products',
      error: err.message,
    });
  }
});

/**
 * Borrowed products with payment method expiring in 30 days
 */
app.get('/product-borrowed-expired', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT 
        t.product_id,
        t.evt_type,
        t.transaction_id,
        t.user_id,
        ue.meta::jsonb->>'valid_until' AS valid_until
      FROM (
        SELECT DISTINCT ON (transaction_id) *
        FROM product_events
        ORDER BY transaction_id, evt_date DESC
      ) t
      INNER JOIN (
        SELECT DISTINCT ON (user_id) *
        FROM user_events
        WHERE evt_type = 'add-payment-method'
          AND meta IS NOT NULL
        ORDER BY user_id, created DESC
      ) ue ON t.user_id = ue.user_id
      WHERE t.evt_type = 'borrow'
        AND (
          to_date(
            '20' || split_part(ue.meta::jsonb->>'valid_until', '/', 2)
            || '-' ||
            split_part(ue.meta::jsonb->>'valid_until', '/', 1)
            || '-01',
            'YYYY-MM-DD'
          ) + INTERVAL '1 month - 1 day'
        )::date <= CURRENT_DATE + 30;
    `);

    res.status(200).json({
      status: 'success',
      message: 'Successfully retrieved borrowed products nearing expiration',
      data: rows,
    });
  } catch (err) {
    console.error('[PRODUCT_BORROWED_EXPIRED_ERROR]', err);

    res.status(500).json({
      status: 'error',
      message: 'Failed to retrieve borrowed products nearing expiration',
      error: err.message,
    });
  }
});



app.listen(3000, () => {
  console.log('API running on port 3000');
});
