-- ============================================================
-- SAMPLE CONFIG: Registering a table in the framework
-- Insert once — framework handles everything from here
-- ============================================================

-- ── 1. Register source→target pair ─────────────────────────
INSERT INTO `your-gcp-project.framework_config.table_registry` VALUES (
  'sales_orders_v1',              -- registry_id
  'source-project',               -- source_project
  'raw_data',                     -- source_dataset
  'sales_orders',                 -- source_table
  'target-project',               -- target_project
  'curated',                      -- target_dataset
  'sales_orders',                 -- target_table
  ['order_id', 'tenant_id'],      -- primary_key_cols (composite PK)
  ['order_id', 'tenant_id', 'customer_id', 'amount', 'status', 'updated_at'],  -- delta_hash_cols
  -- Use [] or NULL here to hash ALL columns ('*' mode):
  -- [],
  'updated_at',                   -- watermark_col
  TRUE,                           -- is_active
  NULL,                           -- batch_size (NULL = no limit)
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

-- ── 2. Register DQ rules for this table ────────────────────

-- Rule 1: order_id must never be null (REJECT — blocks pipeline)
INSERT INTO `your-gcp-project.framework_config.dq_rules` VALUES (
  'dq_sales_orders_001',
  'sales_orders_v1',
  'order_id_not_null',
  'NOT_NULL',
  'order_id',
  NULL,                           -- no params needed for NOT_NULL
  'REJECT',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Rule 2: amount must be between 0 and 10,000,000 (REJECT)
INSERT INTO `your-gcp-project.framework_config.dq_rules` VALUES (
  'dq_sales_orders_002',
  'sales_orders_v1',
  'amount_valid_range',
  'RANGE',
  'amount',
  JSON '{"min": 0, "max": 10000000, "inclusive": true}',
  'REJECT',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Rule 3: status must match valid values (QUARANTINE — bad rows removed)
INSERT INTO `your-gcp-project.framework_config.dq_rules` VALUES (
  'dq_sales_orders_003',
  'sales_orders_v1',
  'status_valid_regex',
  'REGEX',
  'status',
  JSON '{"pattern": "^(PENDING|CONFIRMED|SHIPPED|DELIVERED|CANCELLED)$"}',
  'QUARANTINE',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Rule 4: order_id must be unique in delta batch (WARN — logs but continues)
INSERT INTO `your-gcp-project.framework_config.dq_rules` VALUES (
  'dq_sales_orders_004',
  'sales_orders_v1',
  'order_id_unique_in_batch',
  'UNIQUENESS',
  NULL,
  JSON '{"scope_cols": ["order_id", "tenant_id"]}',
  'WARN',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Rule 5: customer_id must exist in customers table (QUARANTINE)
INSERT INTO `your-gcp-project.framework_config.dq_rules` VALUES (
  'dq_sales_orders_005',
  'sales_orders_v1',
  'customer_id_referential_check',
  'REFERENTIAL',
  'customer_id',
  JSON '{"ref_project": "target-project", "ref_dataset": "curated", "ref_table": "customers", "ref_column": "customer_id"}',
  'QUARANTINE',
  TRUE,
  CURRENT_TIMESTAMP()
);

-- Rule 6: Custom SQL — no negative amounts with DELIVERED status
INSERT INTO `your-gcp-project.framework_config.dq_rules` VALUES (
  'dq_sales_orders_006',
  'sales_orders_v1',
  'no_negative_delivered_orders',
  'CUSTOM_SQL',
  NULL,
  JSON '{"sql": "SELECT COUNT(*) FROM {table} WHERE amount < 0 AND status = \'DELIVERED\'"}',
  'REJECT',
  TRUE,
  CURRENT_TIMESTAMP()
);


-- ============================================================
-- EXAMPLE 2: A different table — full row hash ('*' mode)
-- No need to specify columns — framework hashes entire row
-- ============================================================

INSERT INTO `your-gcp-project.framework_config.table_registry` VALUES (
  'product_catalog_v1',
  'source-project', 'raw_data', 'products',
  'target-project', 'curated', 'products',
  ['product_id'],                 -- simple single-column PK
  [],                             -- EMPTY = use ALL columns for hash ('*' mode)
  'modified_at',
  TRUE, NULL,
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Minimal DQ for products table
INSERT INTO `your-gcp-project.framework_config.dq_rules` VALUES
  ('dq_product_001', 'product_catalog_v1', 'product_id_not_null', 'NOT_NULL', 'product_id', NULL, 'REJECT', TRUE, CURRENT_TIMESTAMP()),
  ('dq_product_002', 'product_catalog_v1', 'price_positive', 'RANGE', 'price', JSON '{"min": 0}', 'QUARANTINE', TRUE, CURRENT_TIMESTAMP());
