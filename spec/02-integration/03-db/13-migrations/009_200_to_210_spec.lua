local helpers = require "spec.helpers"
local fmt = string.format

local PG_HAS_COLUMN_SQL = [[
  SELECT *
  FROM information_schema.columns
  WHERE table_schema = 'public'
  AND table_name     = '%s'
  AND column_name    = '%s';
]]

local PG_HAS_CONSTRAINT_SQL = [[
  SELECT *
  FROM pg_catalog.pg_constraint
  WHERE conname = '%s';
]]

local PG_HAS_INDEX_SQL = [[
  SELECT *
  FROM pg_indexes
  WHERE indexname = '%s';
]]

local function assert_pg_has_column(cn, table_name, column_name, data_type)
  local res = assert(cn:query(fmt(PG_HAS_COLUMN_SQL, table_name, column_name)))

  assert.equals(1, #res)
  assert.equals(column_name, res[1].column_name)
  assert.equals(string.lower(data_type), string.lower(res[1].data_type))
end


local function assert_not_pg_has_column(cn, table_name, column_name, data_type)
  local res = assert(cn:query(fmt(PG_HAS_COLUMN_SQL, table_name, column_name)))
  assert.same({}, res)
end


local function assert_pg_has_constraint(cn, constraint_name)
  local res = assert(cn:query(fmt(PG_HAS_CONSTRAINT_SQL, constraint_name)))

  assert.equals(1, #res)
  assert.equals(constraint_name, res[1].conname)
end


local function assert_not_pg_has_constraint(cn, constraint_name)
  local res = assert(cn:query(fmt(PG_HAS_CONSTRAINT_SQL, constraint_name)))
  assert.same({}, res)
end


local function assert_pg_has_index(cn, index_name)
  local res = assert(cn:query(fmt(PG_HAS_INDEX_SQL, index_name)))

  assert.equals(1, #res)
  assert.equals(index_name, res[1].indexname)
end


local function assert_not_pg_has_index(cn, index_name)
  local res = assert(cn:query(fmt(PG_HAS_INDEX_SQL, index_name)))
  assert.same({}, res)
end


local function assert_pg_has_fkey(cn, table_name, column_name)
  assert_pg_has_column(cn, table_name, column_name, "uuid")
  assert_pg_has_constraint(cn, table_name .. "_" .. column_name .. "_fkey")
end


local function assert_not_pg_has_fkey(cn, table_name, column_name)
  assert_not_pg_has_column(cn, table_name, column_name, "uuid")
  assert_not_pg_has_constraint(cn, table_name .. "_" .. column_name .. "_fkey")
end


describe("#db migration core/009_200_to_210 spec", function()
  local _, db

  after_each(function()
    -- Clean up the database schema after each exercise.
    -- This prevents failed migration tests impacting other tests in the CI
    assert(db:schema_reset())
  end)

  describe("#postgres", function()
    before_each(function()
      _, db = helpers.get_db_utils("postgres", nil, nil, {
        stop_namespace = "kong.db.migrations.core",
        stop_migration = "007_140_to_150"
      })
    end)

    it("adds/removes columns and constraints", function()
      local cn = db.connector
      assert_pg_has_constraint(cn, "ca_certificates_cert_key")
      assert_not_pg_has_column(cn, "ca_certificates", "cert_digest", "text")
      assert_not_pg_has_column(cn, "services", "tls_verify", "boolean")
      assert_not_pg_has_column(cn, "services", "tls_verify_depth", "smallint")
      assert_not_pg_has_column(cn, "services", "ca_certificates", "array")
      assert_not_pg_has_fkey(cn, "upstreams", "client_certificate_id")
      assert_not_pg_has_index(cn, "upstreams_fkey_client_certificate")

      -- kong migrations up
      assert(helpers.run_up_migration(db, "core", "kong.db.migrations.core", "009_200_to_210"))

      -- MIGRATING/AFTER
      assert_not_pg_has_constraint(cn, "ca_certificates_cert_key")
      assert_pg_has_column(cn, "ca_certificates", "cert_digest", "text")
      assert_pg_has_column(cn, "services", "tls_verify", "boolean")
      assert_pg_has_column(cn, "services", "ca_certificates", "array")
      assert_pg_has_fkey(cn, "upstreams", "client_certificate_id")
      assert_pg_has_index(cn, "upstreams_fkey_client_certificate")
    end)
  end)

  --[[


    local cn = db.connector
    local res = assert(cn:query([[
      SELECT *
      FROM information_schema.columns
      WHERE table_schema = 'public'
      AND table_name     = 'routes'
      AND column_name    = 'path_handling';
    ))
    assert.same({}, res)

    -- kong migrations up
    assert(helpers.run_up_migration(db, "core", "kong.db.migrations.core", "007_140_to_150"))

    res = assert(cn:query([[
      SELECT *
      FROM information_schema.columns
      WHERE table_schema = 'public'
      AND table_name     = 'routes'
      AND column_name    = 'path_handling';
    ))
    assert.equals(1, #res)
    assert.equals("routes", res[1].table_name)
    assert.equals("path_handling", res[1].column_name)
    -- migration has no `teardown` in postgres, no further tests needed
  end)

  it("#cassandra", function()
    local uuid = "c37d661d-7e61-49ea-96a5-68c34e83db3a"

    _, db = helpers.get_db_utils("cassandra", nil, nil, {
      stop_namespace = "kong.db.migrations.core",
      stop_migration = "007_140_to_150"
    })

    local cn = db.connector

    -- BEFORE
    assert(cn:query(fmt([[
      INSERT INTO
      routes (partition, id, name, paths)
      VALUES('routes', %s, 'test', ['/']);
    , uuid)))

    local res = assert(cn:query(fmt([[
      SELECT * FROM routes WHERE partition = 'routes' AND id = %s;
    , uuid)))
    assert.same(1, #res)
    assert.same(uuid, res[1].id)
    assert.is_nil(res[1].path_handling)

    -- kong migrations up
    assert(helpers.run_up_migration(db, "core", "kong.db.migrations.core", "007_140_to_150"))

    -- MIGRATING
    res = assert(cn:query(fmt([[
      SELECT * FROM routes WHERE partition = 'routes' AND id = %s;
    , uuid)))
    assert.same(1, #res)
    assert.same(uuid, res[1].id)
    assert.is_nil(res[1].path_handling)

    -- kong migrations finish
    assert(helpers.run_teardown_migration(db, "core", "kong.db.migrations.core", "007_140_to_150"))

    -- AFTER
    res = assert(cn:query(fmt([[
      SELECT * FROM routes WHERE partition = 'routes' AND id = %s;
    , uuid)))
    assert.same(1, #res)
    assert.same(uuid, res[1].id)
    assert.same("v1", res[1].path_handling)
  end)
  ]]
end)
