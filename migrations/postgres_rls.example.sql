-- Optional PostgreSQL reference only. The current runtime does not set
-- app.tenant_id and MUST NOT apply this script without transaction-scoping
-- middleware plus real non-owner-role tests.
-- Each tenant transaction must first execute:
--   SELECT set_config('app.tenant_id', '<tenant>', true);
-- Administrative transactions additionally use a separately privileged role.

DO $$
DECLARE
  table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'sessions', 'session_events', 'memories', 'summaries', 'artifacts',
    'knowledge_chunks', 'audit_logs', 'tenant_usage', 'usage_reservations',
    'tenant_concurrency_slots',
    'inbound_receipts', 'outbox', 'session_leases'
  ]
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
    EXECUTE format('ALTER TABLE %I NO FORCE ROW LEVEL SECURITY', table_name);
    EXECUTE format('DROP POLICY IF EXISTS tenant_scope ON %I', table_name);
    EXECUTE format(
      'CREATE POLICY tenant_scope ON %I USING '
      '(tenant_id = current_setting(''app.tenant_id'', true)) '
      'WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))',
      table_name
    );
  END LOOP;
END $$;
