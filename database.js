const oracledb = require('oracledb');
const { EventEmitter } = require('events');
const dbconfig = require('./dbconfig');

class DB extends EventEmitter {
  initialized = false;
  dbsPool = {};
  commandsQueue = [];

  async initialize() {
    try {
      for (let db of Object.keys(dbconfig)) {
        let config = dbconfig[db];
        this.dbsPool[config.poolAlias] = await oracledb.createPool(config);
      }

      this.initialized = true;
      this.emit('initialized');
      this.commandsQueue.forEach(command => command());
      this.commandsQueue = [];
    } catch (err) {
      console.log(err);
    }
  }

  async doExecute(poolAlias, statement, binds = [], opts = {}) {
    if (!this.initialized) {
      console.log(`Request queued: ${poolAlias} ${statement} ${binds}`);

      return new Promise((resolve, reject) => {
        const command = () => {
          this.doExecute(poolAlias, statement, binds, opts)
            .then(resolve, reject);
        };
        this.commandsQueue.push(command);
      });
    }

    return new Promise(async (resolve, reject) => {
        let conn;
        opts.outFormat = oracledb.OBJECT;
        opts.autoCommit = true;
        try {
          conn = await oracledb.getConnection(this.dbsPool[poolAlias]);
          const result = await conn.execute(statement, binds, opts);
          resolve(result);
        }
        catch (err) { reject({ error: err }); }
        finally {
          if (conn) {
            try {
              await conn.close();
            } catch (err) { 
              console.log(err);
            }
          }
        }
      }
    );
  }

  async doExecuteMany(poolAlias, statement, binds = [], opts = {}) {
    if (!this.initialized) {
      console.log(`Request queued: ${poolAlias} ${statement} ${binds}`);

      return new Promise((resolve, reject) => {
        const command = () => {
          this.doExecuteMany(poolAlias, statement, binds, opts)
            .then(resolve, reject);
        };
        this.commandsQueue.push(command);
      });
    }

    return new Promise(async (resolve, reject) => {
      let conn;
      opts.outFormat = oracledb.OBJECT;
      opts.autoCommit = true;
      opts.batchErrors = true;
      try {
        conn = await oracledb.getConnection(this.dbsPool[poolAlias]);
        const result = await conn.executeMany(statement, binds, opts);
        resolve(result);
      }
      catch (err) { reject(err); }
      finally {
        if (conn) {
          try { 
            await conn.close();
          } catch (err) {
            console.log(err);
          }
        }
      }
    });
  }

  async close() {
    try {
      Object.keys(dbconfig).forEach( async (db) => await oracledb.getPool(dbconfig[db].poolAlias).close());
    }
    catch (err) {
      console.log(err);
    }
  }
}

module.exports = new DB();