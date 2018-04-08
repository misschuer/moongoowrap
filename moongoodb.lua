local moongoo = require("resty.moongoo")

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 42)
_M._VERSION = '0.01'


local commands = {
    'insert',
    'remove',
    'update',
}

local function get_connection(db_string)
    return moongoo.new(db_string)
end

local function get_collection(conn, db_name, table_name)
    return conn:db(db_name):collection(table_name)
end

local function setkeepalive(conn)
    conn:close()
end

local mt = { __index = _M }

local function convert_data(data)
    for key, val in pairs(data) do
        if key == '_id' then
            data[key] = nil
        elseif type(val) == 'userdata' then
            data[key] = tonumber(tostring(val))
        end
    end
    return data
end

function _M.find_one( self, query, fields)
    local conn, err = get_connection(self.db_string, self.db_name, self.table_name)
    if not conn then
        return nil, err
    end
    
    local col = get_collection(conn, self.db_name, self.table_name)
    local cursor = col:find(query, fields)
    local ret, err = cursor:limit(-1):next()
    if err and string.lower(err) ~= 'no more data' then
        return nil, err
    end
    if ret then
        ret = convert_data(ret)
    end
    setkeepalive(conn)

    return ret
end

function _M.find( self, query, fields, sorts, limits, skips)
    local conn, err = get_connection(self.db_string, self.db_name, self.table_name)
    if not conn then
        return nil, err
    end
    
    local col = get_collection(conn, self.db_name, self.table_name)
    local cursor = col:find(query, fields)
    cursor = sorts and cursor:sort(sorts) or cursor
    cursor = limits and cursor:limit(limits) or cursor
    cursor = skips and cursor:skip(skips) or cursor
    
    local results = {}
    while true do
        local data, err = cursor:next()
        if err and string.lower(err) ~= 'no more data' then
            return nil, err
        end
        if not data then
            break
        end
        data = convert_data(data)
        table.insert(results, data)
    end
    
    setkeepalive(conn)

    return results
end

function _M.count( self, query, limits, skips)
    local conn, err = get_connection(self.db_string, self.db_name, self.table_name)
    if not conn then
        return nil, err
    end
    
    local col = get_collection(conn, self.db_name, self.table_name)
    local cursor = col:find(query)
    cursor = limits and cursor:limit(limits) or cursor
    cursor = skips and cursor:skip(skips) or cursor
    
    local count, err = cursor:count()
    if err then
        return nil, err
    end
    setkeepalive(conn)
    return tonumber(tostring(count))
end

local function do_command(self, cmd, ... )
    local conn, err = get_connection(self.db_string)
    if not conn then
        return nil, err
    end

    local col = get_collection(conn, self.db_name, self.table_name)
    local fun = col[cmd]
    local result, err = fun(col, ...)
    if not result or err and type(err) == 'string' then
        return nil, err
    end
    
    setkeepalive(conn)
    return result, nil
end

function _M.new(self, config)
    config = config or {}
    config.db_string = config.db_string or conf.db_string
    config.db_name    = config.db_name or conf.db_name
    config.table_name = config.table_name or 'test'
    
    for i = 1, #commands do
        local cmd = commands[i]
        _M[cmd] =
            function (self, ...)
                return do_command(self, cmd, ...)
            end
    end

    return setmetatable({
        db_string   = config.db_string,
        db_name     = config.db_name,
        table_name  = config.table_name,
    }, mt)
end

return _M
