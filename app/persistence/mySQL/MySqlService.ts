import * as Client from 'mysql';
import { Sequelize } from 'sequelize';
import { helper } from '../../common/helper';

const logger = helper.getLogger('MySqlService');

/**
 *
 *
 * @class MySqlService
 */
export class MySqlService {
	mysqlconfig: any;
	userModel: any;
	client: Client;

	/**
	 * Create an instance of MySqlService
	 * @param {*} mysqlconfig
	 * @memberof MySqlService
	 */
	constructor(mysqlconfig: {
		host: any;
		port: any;
		database: any;
		username: any;
		passwd: any;
	}) {
		this.mysqlconfig = mysqlconfig;
		this.mysqlconfig.host = process.env.DATABASE_HOST || mysqlconfig.host;
		this.mysqlconfig.port = process.env.DATABASE_PORT || mysqlconfig.port;
		this.mysqlconfig.database =
			process.env.DATABASE_DATABASE || mysqlconfig.database;
		this.mysqlconfig.user = process.env.DATABASE_USERNAME || mysqlconfig.username;
		this.mysqlconfig.password = process.env.DATABASE_PASSWD || mysqlconfig.passwd;
		this.userModel = null;

		// don't log password
		const connectionString = `mysql://${this.mysqlconfig.username}:******@${this.mysqlconfig.host}:${this.mysqlconfig.port}/${this.mysqlconfig.database}`;

		logger.info(`connecting to Postgresql ${connectionString}`);

		this.client = new Client(this.mysqlconfig);
	}

	/**
	 *
	 * Create and return the instance for accessing User table via Sequelize
	 * @param {*} attributes
	 * @param {*} options
	 * @returns {Sequelize.Model} Newly defined model
	 * @memberof MySqlService
	 */
	getUserModel(attributes, options) {
		const sequelize = new Sequelize(
			`mysql://${this.mysqlconfig.user}:${this.mysqlconfig.password}@${this.mysqlconfig.host}:${this.mysqlconfig.port}/${this.mysqlconfig.database}`,
			{ logging: false }
		);
		this.userModel = sequelize.define('users', attributes, options);
		return this.userModel;
	}

	/**
	 *
	 *
	 * @memberof MySqlService
	 */
	async handleDisconnect() {
		try {
			this.client.on('error', (err: NodeJS.ErrnoException) => {
				logger.error('db error', err);
				if (err.code === 'PROTOCOL_CONNECTION_LOST') {
					this.handleDisconnect();
				} else {
					throw err;
				}
			});
			await this.client.connect();
		} catch (err) {
			if (err) {
				/*
				 * We introduce a delay before attempting to reconnect,
				 * To avoid a hot loop, and to allow our node script to
				 * Process asynchronous requests in the meantime.
				 */
				logger.error('error when connecting to db:', err);
				setTimeout(this.handleDisconnect, 2000);
			}
		}
	}

	/**
	 *
	 *
	 * @memberof MySqlService
	 */
	openconnection() {
		this.client.connect();
	}

	/**
	 *
	 *
	 * @memberof MySqlService
	 */
	closeconnection() {
		this.client.end();
	}

	/**
	 *
	 *
	 * @param {*} tablename
	 * @param {*} columnValues
	 * @returns
	 * @memberof MySqlService
	 */
	saveRow(tablename, columnValues) {
		return new Promise((resolve, reject) => {
			const addSqlParams = [];
			const updatesqlcolumn = [];
			const updatesqlflag = [];
			let i = 1;
			Object.keys(columnValues).forEach(k => {
				const v = columnValues[k];
				addSqlParams.push(v);
				updatesqlcolumn.push(JSON.stringify(k));
				updatesqlflag.push(`$${i}`);
				i += 1;
			});

			const updatesqlparmstr = updatesqlcolumn.join(',');
			const updatesqlflagstr = updatesqlflag.join(',');
			const addSql = `INSERT INTO ${tablename}  ( ${updatesqlparmstr} ) VALUES( ${updatesqlflagstr}  ) RETURNING *;`;
			logger.debug(`Insert sql is ${addSql}`);
			//   Console.log(`Insert sql is ${addSql}`);
			this.client.query(addSql, addSqlParams, (err, res) => {
				if (err) {
					logger.error('[INSERT ERROR] - ', err.message);
					reject(err);
					return;
				}

				logger.debug(
					'--------------------------INSERT----------------------------'
				);
				//  Console.log('INSERT ID:', res.rows[0].id);
				logger.debug(
					'-----------------------------------------------------------------'
				);

				resolve(res.rows[0].id);
			});
		});
	}

	/**
	 * Update table
	 *
	 * @param String        tablename  the table name.
	 * @param String array  columnAndValue  the table column and value Map.
	 * @param String        pkName   the primary key name.
	 * @param String        pkValue  the primary key value.
	 *
	 *
	 */
	updateRowByPk(tablename, columnAndValue, pkName, pkValue) {
		return new Promise((resolve, reject) => {
			const addSqlParams = [];
			const updateParms = [];
			Object.keys(columnAndValue).forEach(k => {
				const v = columnAndValue[k];
				addSqlParams.push(v);
				updateParms.push(`${k} = ?`);
			});

			const searchparm = {
				pkName: pkValue
			};

			Object.keys(searchparm).forEach(k => {
				const v = searchparm[k];
				addSqlParams.push(v);
			});

			const updateParmsStr = updateParms.join(',');

			const addSql = ` UPDATE ${tablename} set ${updateParmsStr} WHERE ${pkName} = ${pkValue} RETURNING *`;

			logger.debug(`update sql is ${addSql}`);
			this.client.query(addSql, addSqlParams, (err, res) => {
				if (err) {
					logger.error('[INSERT ERROR] - ', err.message);
					reject(err);
					return;
				}

				logger.debug(
					'--------------------------UPDATE----------------------------'
				);
				logger.debug(' update result :', res);
				logger.debug(
					'-----------------------------------------------------------------\n\n'
				);

				resolve(res.rows);
			});
		});
	}
}
