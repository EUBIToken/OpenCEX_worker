"use strict";
console.log('OpenCEX Worker v1.0 - the OpenCEX worker process');
console.log('Made by Jessie Lesbian');
console.log('Email: jessielesbian@protonmail.com Reddit: https://www.reddit.com/u/jessielesbian');
console.log('');
{
	const env = process.env;
	let _sql;
	let _sqlescape;
	{
		const mysql = require('mysql');
		_sql = mysql.createConnection({host: env.OpenCEX_sql_servername, user: env.OpenCEX_sql_username, password: env.OpenCEX_sql_password, database: (env.OpenCEX_devserver === "true") ? "OpenCEX_test" : "OpenCEX", port: 3306, ssl:{ca:require('fs').readFileSync('certificate.txt')}});
		_sqlescape = mysql.escape;
	}
	const sql = _sql;
	_sql = undefined;
	const sqlescape = _sqlescape;
	_sqlescape = undefined;
	
	//Requests made before SQL is connected are admitted here!
	let beforesqlavail = [];
	
	sql.connect(async function(err) {
		if (err) throw err;
		beforesqlavail.forEach(async function(f){
			f();
		});
		beforesqlavail = undefined;
	});
	
	let SQL_locked = false;
	const SQL_queue = [];
	const lockSQL = function(f){
		if(SQL_locked){
			SQL_queue.push(f);
		} else{
			SQL_locked = true;
			f();
		}
	};
	
	const awaitSQL = async function(f){
		const realcall = async function(){
			lockSQL(f);
		};
		if(beforesqlavail){
			beforesqlavail.push(realcall);
		} else{
			realcall();
		}
	};
	
	
	
	//NOTE: f should not be an asynchronous function.
	const useSQL = async function(response, f){
		awaitSQL(async function(){
			let jobid = undefined;
			//Core safety methods
			const unlockSQL = async function(){
				//If there is nothing waiting to use SQL, unlock!
				//otherwise, execute next SQL-using task without releasing and reacquiring lock!
				const shift = SQL_queue.shift();
				if(shift){
					shift();
				} else{
					SQL_locked = false;
				}
			};
			
			let connection_open = !!response; //If we are in connectionless mode, then this would come in handy!
			let _tempfuncexport;
			{
				const res2 = response;
				_tempfuncexport = function(msg){
					sql.query("ROLLBACK;");
					unlockSQL();
					if(connection_open){
						res2.write(JSON.stringify({error: msg.toString()}));
						res2.end();
						connection_open = false;
						
					}
					
					//Throw to abort calling method
					//NOTE: calling method must return after catching exception!
					throw "";
				};
			}
			const fail = _tempfuncexport;
			{
				const res2 = response;
				_tempfuncexport = async function(data, nocommit){
					const handle3 = async function(err){
						try{
							checkSafety2(err, "Unable to commit MySQL transaction!");	
						} catch (e){
							return;
						}
						if(connection_open){
							res2.write(JSON.stringify({returns: data}));
							res2.end();
							connection_open = false;
						}						
					};
					const handle2 = async function(err){
						try{
							checkSafety2(err, "Unable to commit MySQL transaction!");	
						} catch (e){
							return;
						}
						
						sql.query("COMMIT;", handle3);
						unlockSQL();
					};
					const handle = async function(err){
						if(nocommit){
							try{
								checkSafety2(err, "Unable to update task status!");	
							} catch (e){
								return;
							}
							
							sql.query("UNLOCK TABLES;", handle2);
							
						} else{
							handle3(false);
						}
						
					};
					if(nocommit){
						handle(false);
					} else{
						sql.query(["UPDATE WorkerTasks SET Status = 1 WHERE Id = ", sqlescape(jobid), ";"].join(""), handle);
					}
				};
			}
			response = undefined;
			const safeQuery = async function(query, callback){
				sql.query(query, async function(err, res){
					try{
						checkSafety2(err, "SQL Query returned error!");
					} catch{
						console.log("sqlerror: " + query);
						return;
					}
					callback(res);
				});
			};
			
			//Extended safety methods
			
			const checkSafety2 = function(exp, msg){
				if(exp){
					fail(msg);
				}
			};
			
			safeQuery("START TRANSACTION;", async function(){
				safeQuery("LOCK TABLES Balances WRITE, WorkerTasks WRITE;", async function(){
					f(fail, function(exp, msg){
						if(!exp){
							fail(msg);
						}
					}, checkSafety2, safeQuery, _tempfuncexport, function(ji){
						try{
							checkSafety2(jobid, "Job ID already set!");
						} catch{
							return;
						}
						jobid = ji;
					});
					_tempfuncexport = undefined;
				});
			
			
		});
	});
	let BigNumber = require('web3-utils').BN;
	
	let http = require('http').createServer(async function(req, res){
		if(req.url.length == 0){
			res.write('{"error": "Invalid request!"}');
			res.end();
		}
		let url = req.url.substring(1);
		const params = url.split('/');
		
		if(params.length < 3){
			res.write('{"error": "Invalid request!"}');
			res.end();
			return;
		} else{
			params.reverse()
			if(decodeURIComponent(params.pop()) !== env.OpenCEX_shared_secret){
				res.write('{"error": "Unauthorized request!"}');
				res.end();
				return;
			}
		}
		useSQL(res, async function(fail, checkSafety, checkSafety2, safeQuery, ret2, setjobid){
			//admit request to task queue (NOTE: strip authorization and initial spacer)
			url = url.substring(url.indexOf("/") + 1);
			safeQuery(["INSERT INTO WorkerTasks (URL, URL2, LastTouched, Status) VALUES (", sqlescape(url.substring(0, 255)), ", ", sqlescape(url.substring(255)), ", ", sqlescape(Date.now().toString()), ", ", "0", ");"].join(''), async function(){
				safeQuery('SELECT LAST_INSERT_ID();', async function(ji){
					setjobid([0]["LAST_INSERT_ID()"]);
					//execute request
					executeRequest(params, res, fail, checkSafety, checkSafety2, safeQuery, ret2);
				});
				
				
			});
			url = undefined;
			
			
		});
		
	});
	const executeRequest = async function(params, res, fail, checkSafety, checkSafety2, safeQuery, ret2){
		const safeshift = function(){
			const result = params.pop();
			checkSafety2(result === undefined, "Not enough parameters!");
			return decodeURIComponent(result);
		};
		const chains = [];
		{
			const eth = require('web3-eth');
			chains.polygon = new eth('https://polygon-rpc.com/');
		}
		const methods = {
			sendAndCreditWhenSecure: async function(){
				
				//auth/method/tx/account/token/amount
				const BlockchainManager = chains[safeshift()];
				try{
					checkSafety(BlockchainManager, "Undefined blockchain!");
				} catch (e){
					console.log(e);
					return;
				}
				
				
				const tx = safeshift();
				const account = safeshift();
				const token = safeshift();
				let _amt = undefined;
				try{
					_amt = new BigNumber(safeshift());
					checkSafety2(parseInt(account) == NaN, "Invalid UserID!");
				} catch(e){
					console.log(e);
					if(amt){
						return;
					} else{
						try{
							//Will-throw guarantee
							fail("Invalid amount!");
							throw "";
						} catch(e){
							return;
						}
					}
				}
				const amount = _amt;
				_amt = undefined;
				
				const promise = BlockchainManager.sendSignedTransaction(tx);
				let lock2 = false;
				const confirmation = async function(n, receipt){
					console.log("confirmations: " + n);
					if(n < 2 || lock2 || !receipt){
						return;
					}
					lock2 = true;
					promise.off('confirmation', confirmation);
					if(receipt.status){
						const selector = [" WHERE Coin = ", sqlescape(token), " AND UserID = ", sqlescape(account), ";"].join("");
						safeQuery("SELECT Balance FROM Balances" + selector, async function(balance){
							let insert = false;
							try{
								console.log(JSON.stringify(balance));
								if(balance.length == 0){
									balance = amount.toString();
									insert = true;
								} else{
									checkSafety(balance.length == 1, "Corrupted balances database!");
									balance = balance[0];
									checkSafety(balance.Balance, "Corrupted balances database!");
									balance = balance.Balance;
									try{
										balance = (new BigNumber(balance)).add(amount).toString();
									} catch (e){
										console.log(e);
										fail("Unable to add BigNumbers!");
									}
								}
								
							} catch (e){
								console.log(e);
								return;
							}
							
							const exit = async function(){
								ret2("");
							};
							
							if(insert){
								safeQuery(["INSERT INTO Balances (Coin, Balance, UserID) VALUES (", sqlescape(token), ", ", sqlescape(balance), ", ", sqlescape(account), ");"].join(""), exit);
							} else{
								safeQuery(["UPDATE Balances SET Balance = ", sqlescape(balance), selector].join(""), exit);
							}
							
							
						});
					}
					return;
				};
				promise.on('confirmation', confirmation);
				
				ret2("", true);
			}
		};
		
		const method = methods[params.pop()];
		if(method){
			res = undefined;
			method();
		} else if(res){
			res.write('{"error": "Invalid request method!"}');
			res.end();
		}
	};
	http.listen(env.PORT || 80);
	
	
	
	//if we get a SIGTERM, stop accepting new requests. Failure to
	//do this nearly ended with the catastrophic failure of the EUBI-bEUBI bridge.
	process.on('SIGTERM', async function(){
		if(http){
			http.close();
			http = null;
			if(beforesqlavail){
				//It's safe to abort ungracefully here, since no requests should
				//be pending at this time
				process.exit();
			}
		}
	});
}