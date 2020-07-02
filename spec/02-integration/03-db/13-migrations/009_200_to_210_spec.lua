local helpers = require "spec.helpers"
local fmt = string.format
local utils = require "kong.tools.utils"

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

local PG_HAS_TABLE_SQL = [[
  SELECT *
  FROM pg_catalog.pg_tables
  WHERE schemaname = 'public'
  AND tablename = '%s';
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


local function assert_pg_has_table(cn, table_name)
  local res = assert(cn:query(fmt(PG_HAS_TABLE_SQL, table_name)))

  assert.equals(1, #res)
  assert.equals(table_name, res[1].tablename)
end


local function assert_not_pg_has_table(cn, table_name)
  local res = assert(cn:query(fmt(PG_HAS_TABLE_SQL, table_name)))
  assert.same({}, res)
end


local function assert_pg_table_has_ws_id(cn, table_name)
  assert_pg_has_fkey(cn, table_name, "ws_id")
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

    it("adds workspaces table and index", function()
      local cn = db.connector
      assert_not_pg_has_table(cn, "workspaces")
      assert_not_pg_has_index(cn, "workspaces_name_idx")

      -- kong migrations up
      assert(helpers.run_up_migration(db, "core", "kong.db.migrations.core", "009_200_to_210"))

      -- MIGRATING
      assert_pg_has_table(cn, "workspaces")
      assert_pg_has_index(cn, "workspaces_name_idx")
    end)

    it("creates default workspace, adds and sets ws_id in all core tables", function()
      local cn = db.connector
      assert_not_pg_has_fkey(cn, "upstreams", "ws_id")

      -- BEFORE
      -- kong migrations up
      assert(helpers.run_up_migration(db, "core", "kong.db.migrations.core", "009_200_to_210"))

      local res = assert(cn:query(fmt([[
        INSERT INTO upstreams(id, name, slots)
        VALUES ('%s', 'test-upstream', 1);
      ]], utils.uuid())))
      assert.same({ affected_rows = 1 }, res)

      -- MIGRATING
      -- check default workspace exists and get its id
      local res = assert(cn:query("SELECT * FROM workspaces"))
      assert.equals(1, #res)
      assert.equals("default", res[1].name)
      assert.truthy(utils.is_valid_uuid(res[1].id))

      local default_ws_id = res[1].id

      assert_pg_has_fkey(cn, "upstreams", "ws_id")
      local res = assert(cn:query([[
        SELECT * FROM upstreams;
      ]]))
      assert.same(1, #res)
      assert.equals("test-upstream", res[1].name)
      assert.equals(default_ws_id, res[1].ws_id)
    end)
  end)

--[[
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
