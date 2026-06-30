/**
 * edgelink-pg-config.js — PG 连接配置节点
 *
 * 职责：存储 PG 连接参数，本身不管理 Pool。
 * Pool 生命周期由 edgelink-pg-store.js 中的模块级 POOLS 管理。
 */

module.exports = function (RED) {
    'use strict';

    function EdgelinkPgConfig(n) {
        RED.nodes.createNode(this, n);
        // DEBUG: 确认配置是否被 Node-RED 正确传入
        console.log('[PG-Config] CREATING — n.name=' + n.name + ' n.host=' + n.host + ' n.port=' + n.port + ' n.database=' + n.database + ' n.user=' + n.user + ' n.password=' + (n.password ? '***' : '(empty)'));

        this.name = n.name || 'PG-本地';
        this.host = n.host || '127.0.0.1';
        this.port = parseInt(n.port, 10) || 5432;
        this.database = n.database || 'ruoyi_pg';
        this.user = n.user || 'postgres';
        this.password = typeof n.password === 'string' ? n.password : String(n.password || '');
        this.maxConnections = parseInt(n.maxConnections, 10) || 10;
        this.idleTimeout = parseInt(n.idleTimeout, 10) || 30000;
    }

    RED.nodes.registerType('edgelink-pg-config', EdgelinkPgConfig);
};
