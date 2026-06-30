/**
 * edgelink-pg-store.js — PG/TimescaleDB 批量写入节点（共通版）
 *
 * 支持三种输入格式（自动识别）：
 *   1. MC 驱动格式: { deviceId, data: { tagId: {rawValue, engValue, quality, ts, regType} } }
 *   2. 批量 rows:   { rows: [{col: val}, ...] }
 *   3. 单行 object: { col1: val1, col2: val2 }
 *
 * 表名来源: msg.tableName > msg.topic > 节点配置 tableName
 * MC 格式额外支持 ${deviceId} 模板替换
 */

module.exports = function (RED) {
    'use strict';

    // ====================================================================
    // 模块级：全局 Pool 管理（key = user@host:port/database）
    // ====================================================================
    var POOLS = {};

    function getPoolKey(cfg) {
        return (cfg.user || 'postgres') + '@' +
               (cfg.host || '127.0.0.1') + ':' +
               (cfg.port || 5432) + '/' +
               (cfg.database || 'ruoyi_pg');
    }

    function getPool(cfg) {
        var key = getPoolKey(cfg);
        if (!POOLS[key]) {
            var pg = require('pg');
            POOLS[key] = {
                pool: new pg.Pool({
                    host: cfg.host || '127.0.0.1',
                    port: cfg.port || 5432,
                    database: cfg.database || 'ruoyi_pg',
                    user: cfg.user || 'postgres',
                    password: typeof cfg.password === 'string' ? cfg.password : String(cfg.password || ''),
                    max: cfg.maxConnections || 10,
                    idleTimeoutMillis: cfg.idleTimeout || 30000
                }),
                refCount: 0
            };
        }
        POOLS[key].refCount++;
        return POOLS[key].pool;
    }

    function releasePool(cfg) {
        var key = getPoolKey(cfg);
        if (!POOLS[key]) return;
        POOLS[key].refCount = Math.max(0, POOLS[key].refCount - 1);
        if (POOLS[key].refCount === 0) {
            POOLS[key].pool.end();
            delete POOLS[key];
        }
    }

    // ====================================================================
    // 模块级：工具函数
    // ====================================================================

    function resolveTableName(template, deviceId) {
        var name = template.replace(/\$\{deviceId\}/g, deviceId || 'unknown');
        name = name.replace(/[^a-zA-Z0-9_]/g, '_');
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
            throw new Error('Invalid table name after sanitize: ' + name);
        }
        return name;
    }

    /** 列名 sanitize */
    function sanitizeColumn(col) {
        var s = String(col).replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s)) s = '_' + s;
        return s;
    }

    /** 构造参数化 INSERT */
    function buildInsertSQL(tableName, columns, rowCount, useOnConflict) {
        var placeholders = [];
        var idx = 1;
        for (var i = 0; i < rowCount; i++) {
            var row = [];
            for (var j = 0; j < columns.length; j++) {
                row.push('$' + idx++);
            }
            placeholders.push('(' + row.join(', ') + ')');
        }
        var sql = 'INSERT INTO ' + tableName +
                  ' (' + columns.join(', ') + ') VALUES ' +
                  placeholders.join(', ');
        if (useOnConflict) {
            sql += ' ON CONFLICT DO NOTHING';
        }
        return sql;
    }

    function flattenRows(rows) {
        var values = [];
        for (var i = 0; i < rows.length; i++) {
            for (var j = 0; j < rows[i].length; j++) {
                values.push(rows[i][j]);
            }
        }
        return values;
    }

    function classifyError(err) {
        if (!err) return 'retry';
        var code = err.code || '';
        if (code === 'ECONNREFUSED' || code === 'ETIMEDOUT' || code.indexOf('08') === 0) {
            return 'connection';
        }
        if (code === '42P01') {
            return 'table_not_found';
        }
        if (code === '22P02') {
            return 'data_error';
        }
        return 'retry';
    }

    // ====================================================================
    // MC 驱动格式：固定 7 列
    // ====================================================================
    var MC_COLUMNS = [
        'insert_time', 'device_id', 'tag_id',
        'register_type', 'raw_value', 'eng_value', 'quality'
    ];

    function parseMCFormat(payload) {
        var deviceId = payload.deviceId || 'unknown';
        var data = payload.data;
        var tagIds = Object.keys(data);
        var rows = [];
        for (var i = 0; i < tagIds.length; i++) {
            var tagId = tagIds[i];
            var tag = data[tagId];
            if (!tag || typeof tag !== 'object') continue;
            rows.push([
                tag.ts ? tag.ts : new Date().toISOString(),
                deviceId,
                tagId,
                tag.regType || '',
                (tag.rawValue != null) ? tag.rawValue : null,
                (tag.engValue != null) ? tag.engValue : null,
                (tag.quality != null) ? parseInt(tag.quality, 10) || 0 : 0
            ]);
        }
        return rows;
    }

    // ====================================================================
    // 通用格式：从 object 提取列名和值
    // ====================================================================
    function parseGenericRow(obj, columns) {
        // columns: ordered list of column names
        var row = [];
        for (var i = 0; i < columns.length; i++) {
            var v = obj[columns[i]];
            row.push((v != null) ? v : null);
        }
        return row;
    }

    // ====================================================================
    // 格式检测
    // ====================================================================
    function detectFormat(payload) {
        // 批量 rows（显式声明）
        if (payload.rows && Array.isArray(payload.rows) && payload.rows.length > 0) {
            return 'batch';
        }
        // MC 驱动：有 .data 且值为嵌套对象
        if (payload.data && typeof payload.data === 'object' && !Array.isArray(payload.data)) {
            var keys = Object.keys(payload.data);
            if (keys.length > 0 && payload.data[keys[0]] !== null &&
                typeof payload.data[keys[0]] === 'object' && !Array.isArray(payload.data[keys[0]])) {
                return 'mc';
            }
        }
        // 纯 object → 单行
        if (typeof payload === 'object' && payload !== null && !Array.isArray(payload)) {
            return 'single';
        }
        return 'unknown';
    }

    // ====================================================================
    // 节点定义
    // ====================================================================
    function EdgelinkPgStore(config) {
        RED.nodes.createNode(this, config);
        var node = this;

        var pgConfigNode = RED.nodes.getNode(config.pgConfig);
        if (!pgConfigNode) {
            node.error('[PG-Store] edgelink-pg-config not found: ' + (config.pgConfig || '(empty)'));
            return;
        }
        var pgConfig = pgConfigNode;

        node.tableName     = config.tableName || 'plc_data';
        node.batchSize     = clampInt(config.batchSize, 100, 1, 4000);  // Fix-3: PG 参数上限 32767，7列×4000=28000 安全
        node.bufferMax     = clampInt(config.bufferMax, 5000, 100, 100000);
        node.flushInterval = clampInt(config.flushInterval, 5000, 0, 300000);
        node.retryBufferMax = clampInt(config.retryBufferMax, 1000, 100, 50000);
        node.retryInterval  = clampInt(config.retryInterval, 5000, 1000, 300000);
        node.autoCreateTable = config.autoCreateTable === true;
        node.useTimescaleDB = config.useTimescaleDB === true;

        // --- 运行时状态 ---
        var _buffer        = [];     // 主缓冲行数组
        var _bufferColumns = null;   // 当前缓冲的列名
        var _bufferTable   = '';     // 当前缓冲的表名
        var _bufferIsMC    = false;  // 当前缓冲是否为 MC 格式
        var _retryBuffer   = [];     // [{ rows, columns, tableName, isMC }]
        var _writing             = false;
        var _tableCreationFailed = {};  // Fix-2: 建表失败的表面，防止无限循环
        var _flushTimer    = null;
        var _retryTimer    = null;
        var _roundStart    = 0;

        _retryTimer = setInterval(retryFlush, node.retryInterval);
        setStatus('green', 'ready');

        function setStatus(fill, text) {
            node.status({ fill: fill, shape: 'dot', text: text });
        }

        // ================================================================
        // 核心：执行批量 INSERT
        // ================================================================
        function executeInsert(rows, columns, tableName, isMC, callback) {
            var pool = getPool(pgConfig);

            pool.connect(function (err, client) {
                if (err) {
                    callback({ ok: false, retry: true, count: 0, error: '[CONNECT] ' + err.message });
                    return;
                }

                // 🔧 Fix-1: 限制单次查询最大执行时间，防止 PG 假死导致 _writing 永远不解锁
                client.query('SET statement_timeout = ' + (node.flushTimeout || 30000), function () {
                    var sql    = buildInsertSQL(tableName, columns, rows.length, isMC);
                    var values = flattenRows(rows);

                    client.query(sql, values, function (err, result) {
                    if (!err) {
                        client.release();
                        callback({ ok: true, retry: false, count: result.rowCount, error: null });
                        return;
                    }

                    var errorType = classifyError(err);

                    // 建表仅 MC 格式生效（且不在失败黑名单中）
                    if (errorType === 'table_not_found' && isMC && node.autoCreateTable && !_tableCreationFailed[tableName]) {
                        createMCTable(client, tableName, function (createErr) {
                            if (createErr) {
                                _tableCreationFailed[tableName] = true;  // Fix-2: 建表失败不再重试
                                client.release();
                                node.warn('[PG-Store] Auto-create table failed: ' + createErr.message);
                                callback({ ok: false, retry: false, count: 0, error: '[TABLE] ' + createErr.message });
                                return;
                            }
                            client.query(sql, values, function (err2, result2) {
                                client.release();
                                if (err2) {
                                    var et2 = classifyError(err2);
                                    callback({ ok: false, retry: (et2 !== 'data_error'), count: 0, error: '[INSERT] ' + err2.message });
                                } else {
                                    callback({ ok: true, retry: false, count: result2.rowCount, error: null });
                                }
                            });
                        });
                        return;
                    }

                    client.release();

                    if (errorType === 'data_error') {
                        node.warn('[PG-Store] Data error (discarded): ' + err.message);
                        callback({ ok: false, retry: false, count: 0, error: '[DATA] ' + err.message });
                    } else if (errorType === 'table_not_found') {
                        node.warn('[PG-Store] Table not found: ' + tableName);
                        callback({ ok: false, retry: false, count: 0, error: '[TABLE] ' + err.message });
                    } else {
                        callback({ ok: false, retry: true, count: 0, error: '[' + errorType.toUpperCase() + '] ' + err.message });
                    }
                });
            }); // SET statement_timeout callback
        }); // pool.connect callback
        }

        // ================================================================
        // MC 格式专用建表
        // ================================================================
        function createMCTable(client, tableName, callback) {
            var sql = 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' +
                'insert_time TIMESTAMPTZ NOT NULL, ' +
                'device_id VARCHAR(64) NOT NULL, ' +
                'tag_id VARCHAR(64) NOT NULL, ' +
                'register_type VARCHAR(8), ' +
                'raw_value NUMERIC, ' +
                'eng_value NUMERIC, ' +
                'quality INTEGER DEFAULT 0, ' +
                'PRIMARY KEY (insert_time, device_id, tag_id)' +
            ')';
            client.query(sql, function (err) {
                if (err) { callback(err); return; }
                var idxShort = tableName.length > 45 ? tableName.substring(0, 45) : tableName;
                var idxSQL = 'CREATE INDEX IF NOT EXISTS idx_' + idxShort + '_dt ON ' +
                    tableName + ' (device_id, tag_id, insert_time DESC)';
                client.query(idxSQL, function (err2) {
                    if (err2) { callback(err2); return; }
                    if (node.useTimescaleDB) {
                        client.query("SELECT create_hypertable('" + tableName + "', 'insert_time', if_not_exists => TRUE)", function () {
                            callback(null);
                        });
                    } else {
                        callback(null);
                    }
                });
            });
        }

        // ================================================================
        // 刷新主缓冲
        // ================================================================
        function flushBuffer(callback) {
            if (_writing) {
                if (typeof callback === 'function') callback();
                return;
            }
            if (_buffer.length === 0) {
                if (typeof callback === 'function') callback();
                return;
            }

            _writing = true;

            var batch   = _buffer.splice(0, _buffer.length);
            var cols    = _bufferColumns;
            var tbl     = _bufferTable;
            var isMC    = _bufferIsMC;

            // 清空后重置
            _bufferColumns = null;
            _bufferTable   = '';
            _bufferIsMC    = false;

            executeInsert(batch, cols, tbl, isMC, function (result) {
                _writing = false;

                if (result.ok) {
                    setStatus('green', 'inserted: ' + result.count);
                    node.send({ payload: { success: true, inserted: result.count, failed: 0, buffered: _buffer.length, tableName: tbl, roundTimeMs: Date.now() - _roundStart } });
                } else if (result.retry) {
                    addToRetryBuffer(batch, cols, tbl, isMC);
                    setStatus('red', 'error: ' + truncate(result.error, 30));
                    node.send({ payload: { success: false, inserted: 0, failed: batch.length, buffered: _retryBuffer.length, error: result.error, tableName: tbl } });
                } else {
                    setStatus('red', 'error: ' + truncate(result.error, 30));
                    node.send({ payload: { success: false, inserted: 0, failed: batch.length, buffered: _retryBuffer.length, error: result.error, tableName: tbl } });
                }

                if (typeof callback === 'function') callback();
            });
        }

        // ================================================================
        // 重试缓冲刷新
        // ================================================================
        function retryFlush() {
            if (_writing) return;
            if (_retryBuffer.length === 0) return;

            _writing = true;
            var entry = _retryBuffer.shift();

            executeInsert(entry.rows, entry.columns, entry.tableName, entry.isMC, function (result) {
                _writing = false;
                if (result.ok) {
                    setStatus('green', 'retry ok: ' + result.count);
                } else if (result.retry) {
                    addToRetryBuffer(entry.rows, entry.columns, entry.tableName, entry.isMC);
                    setStatus('red', 'retry fail');
                } else {
                    node.warn('[PG-Store] Retry discarded (' + entry.rows.length + ' rows): ' + result.error);
                    setStatus('red', 'discarded');
                }
            });
        }

        function addToRetryBuffer(rows, columns, tableName, isMC) {
            if (rows.length === 0) return;
            _retryBuffer.push({ rows: rows, columns: columns, tableName: tableName, isMC: isMC });
            while (_retryBuffer.length > node.retryBufferMax) {
                var d = _retryBuffer.shift();
                node.warn('[PG-Store] retryBuffer full, dropped ' + d.rows.length + ' rows (table=' + d.tableName + ')');
            }
        }

        function resetFlushTimer() {
            if (_flushTimer) clearTimeout(_flushTimer);
            if (node.flushInterval > 0) {
                _flushTimer = setTimeout(flushBuffer, node.flushInterval);
            }
        }

        // ================================================================
        // 确保缓冲列一致：不一致则先 flush
        // ================================================================
        function ensureBuffer(cols, tbl, isMC) {
            var colsKey = cols.join(',');
            var curKey  = _bufferColumns ? _bufferColumns.join(',') : '';
            if (_buffer.length > 0 && (curKey !== colsKey || _bufferTable !== tbl)) {
                flushBuffer();
            }
            _bufferColumns = cols;
            _bufferTable   = tbl;
            _bufferIsMC    = isMC;
        }

        // ================================================================
        // 输入处理 — 自动识别三种格式
        // ================================================================
        node.on('input', function (msg) {
            try {
                _roundStart = Date.now();
                var payload = msg.payload;
                if (!payload || typeof payload !== 'object') {
                    node.warn('[PG-Store] Invalid input: payload must be an object');
                    return;
                }

                // 上游读取失败 → 跳过
                if (payload.success === false) return;

                var format = detectFormat(payload);
                var rows, columns, tableName, isMC;

                if (format === 'mc') {
                    // === MC 驱动格式 ===
                    var deviceId = payload.deviceId || 'unknown';
                    tableName = resolveTableName(msg.tableName || node.tableName, deviceId);
                    rows   = parseMCFormat(payload);
                    columns = MC_COLUMNS;
                    isMC   = true;
                } else if (format === 'batch') {
                    // === 批量 rows 格式 ===
                    tableName = resolveTableName(String(msg.tableName || msg.topic || node.tableName), '');
                    var rawRows = payload.rows;
                    if (!rawRows || !rawRows.length || !rawRows[0]) { node.warn('[PG-Store] Empty rows array, skip'); return; }  // Fix-4
                    columns = Object.keys(rawRows[0]).map(sanitizeColumn);
                    rows = [];
                    for (var i = 0; i < rawRows.length; i++) {
                        rows.push(parseGenericRow(rawRows[i], columns));
                    }
                    isMC = false;
                } else if (format === 'single') {
                    // === 单行 object 格式 ===
                    tableName = resolveTableName(String(msg.tableName || msg.topic || node.tableName), '');
                    columns = Object.keys(payload).map(sanitizeColumn);
                    rows = [parseGenericRow(payload, columns)];
                    isMC = false;
                } else {
                    node.warn('[PG-Store] Unknown payload format — expected .data (MC), .rows (batch), or plain object (single row)');
                    return;
                }

                if (rows.length === 0) {
                    node.warn('[PG-Store] No valid rows extracted');
                    return;
                }

                // 确保缓冲列一致
                ensureBuffer(columns, tableName, isMC);

                // 放入主缓冲
                for (var r = 0; r < rows.length; r++) {
                    _buffer.push(rows[r]);
                }
                if (_buffer.length > node.bufferMax) {
                    var overflow = _buffer.length - node.bufferMax;
                    _buffer.splice(0, overflow);
                    node.warn('[PG-Store] buffer overflow, dropped ' + overflow + ' rows');
                }

                var shouldFlush = _buffer.length >= node.batchSize || msg.flush === true;
                if (shouldFlush) {
                    flushBuffer();
                } else {
                    setStatus('yellow', 'buffer: ' + _buffer.length);
                }

                node.send({ payload: { success: true, inserted: 0, failed: 0, buffered: _buffer.length, tableName: tableName, roundTimeMs: Date.now() - _roundStart, originalData: payload.data || payload } });

                resetFlushTimer();

            } catch (e) {
                node.error('[PG-Store] Input exception: ' + e.message);
            }
        });

        // ================================================================
        // 关闭
        // ================================================================
        node.on('close', function (done) {
            if (_flushTimer) clearTimeout(_flushTimer);
            if (_retryTimer) clearInterval(_retryTimer);
            flushBuffer(function () {
                releasePool(pgConfig);
                setStatus('grey', 'closed');
                if (typeof done === 'function') done();
            });
        });
    }

    function clampInt(value, defaultVal, min, max) {
        var n = parseInt(value, 10);
        if (isNaN(n)) n = defaultVal;
        if (n < min) n = min;
        if (n > max) n = max;
        return n;
    }

    function truncate(str, maxLen) {
        if (!str) return '';
        return str.length > maxLen ? str.substring(0, maxLen) : str;
    }

    RED.nodes.registerType('edgelink-pg-store', EdgelinkPgStore);
};
