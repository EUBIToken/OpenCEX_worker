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
			const unlockSQL_sync = function(){
				//If there is nothing waiting to use SQL, unlock!
				//otherwise, execute next SQL-using task without releasing and reacquiring lock!
				const shift = SQL_queue.shift();
				if(shift){
					shift();
				} else{
					SQL_locked = false;
					console.log("SQL unlocked!");
				}
				
			};
			const unlockSQL = async function(){
				unlockSQL_sync();
			};
			
			let connection_open = !!response; //If we are in connectionless mode, then this would come in handy!
			let _tempfuncexport;
			{
				const res2 = response;
				_tempfuncexport = function(msg){
					sql.query("ROLLBACK;", unlockSQL);
					if(connection_open){
						res2.write(JSON.stringify({error: msg.toString()}));
						res2.end();
						connection_open = false;
						
					}
					
					console.log("ASSERTION FAILURE: " + msg);
					
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
						} catch {
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
						} catch {
							return;
						}
						
						sql.query("COMMIT;", async function(err){
							if(err){
								handle3(true);
							} else{
								unlockSQL_sync();
								handle3(false);
							}
							
							
						});
						
					};
					const handle = async function(err){
						try{
							checkSafety2(err, "Unable to update task status!");	
						} catch {
							return;
						}
						
						sql.query("UNLOCK TABLES;", handle2);
						
					};
					if(nocommit){
						handle3(false);
					} else{
						sql.query(["DELETE FROM WorkerTasks WHERE Id = ", sqlescape(jobid), ";"].join(""), handle);
					}
				};
			}
			response = undefined;
			const safeQuery = async function(query, callback){
				sql.query(query, async function(err, res){
					try{
						checkSafety2(err, "SQL Query returned error!");
					} catch{
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
			});
		});
	};
			
	
	let web3utils = require('web3-utils');
	const BigNumber = web3utils.BN;
	const web3_sha3 = web3utils.sha3;
	web3utils = undefined;
	
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
					safeQuery('COMMIT;', async function(){
						safeQuery('START TRANSACTION;', async function(){
							setjobid(ji[0]["LAST_INSERT_ID()"]);
							//execute request
							executeRequest(params, res, fail, checkSafety, checkSafety2, safeQuery, ret2, ji[0]["LAST_INSERT_ID()"]);
						});
					});
				});
				
				
			});
			url = undefined;
			
			
		});
		
	});
	const executeRequest = async function(params, res, fail, checkSafety, checkSafety2, safeQuery, ret2, jobid){
		const safeshift = function(){
			const result = params.pop();
			checkSafety2(result === undefined, "Not enough parameters!");
			return decodeURIComponent(result);
		};
		const chains = [];
		{
			const eth = require('web3-eth');
			chains.polygon = new eth('https://polygon-rpc.com/');
			chains.mintme = new eth('https://node1.mintme.com:443');
		}
		let jobAborted = false;
		setTimeout(async function(){
			jobAborted = true;
		}, 600000);
		const methods = {
			sendAndCreditWhenSecure: async function(){
				console.log("sendAndCreditWhenSecure");
				
				//auth/method/tx/account/token/amount
				const BlockchainManager = chains[safeshift()];
				try{
					checkSafety2(jobAborted, "Job timed out!");
					checkSafety(BlockchainManager, "Undefined blockchain!");
				} catch{
					return;
				}
				
				
				const tx = safeshift();
				const account = safeshift();
				const token = safeshift();
				let _amt = undefined;
				try{
					_amt = new BigNumber(safeshift());
					checkSafety2(parseInt(account) == NaN, "Invalid UserID!");
					checkSafety2(jobAborted, "Job timed out!");
				} catch{
					if(amt){
						return;
					} else{
						try{
							//Will-throw guarantee
							fail("Invalid amount!");
							throw "";
						} catch{
							return;
						}
					}
				}
				const amount = _amt;
				_amt = undefined;
				try{
					checkSafety2(jobAborted, "Job timed out!");
				} catch{
					return;
				}
				
				const innerCompartment = async function(promise){
					let lock2 = false;
					const confirmation = async function(n, receipt){
						if(n < 2 || lock2 || !receipt){
							return;
						}
						lock2 = true;
						promise.off('confirmation', confirmation);
						if(receipt.status){
							const selector = [" WHERE Coin = ", sqlescape(token), " AND UserID = ", sqlescape(account), ";"].join("");
							safeQuery("LOCK TABLE Balances WRITE, WorkerTasks WRITE;", async function(){
								safeQuery("SELECT Balance FROM Balances" + selector, async function(balance){
									let insert = false;
									try{
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
											} catch {
												fail("Unable to add BigNumbers!");
											}
										}
										
									} catch {
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
							});
						} else{
							ret2("");
						}
						return;
					};
					if(res){
						promise.on('confirmation', confirmation);
					} else{
						const hash = web3_sha3(tx, { encoding: "hex" });
						const interval = setInterval(async function(){
							console.log("interval");
							if(jobAborted || lock2){
								clearInterval(interval);
							} else{
								BlockchainManager.getTransactionReceipt(hash, async function(receipt){
									if(!receipt){
										return;
									}
									if(!receipt.blockNumber){
										return;
									}
									BlockchainManager.getBlockNumber(async function(blocknumber2){
										if(blocknumber2){
											confirmation(receipt, (new BigNumber(blocknumber2)).sub(new BigNumber(receipt.blockNumber)).toString());
										} else{
											return;
										}
									});
								});
							}
							
						}, 1000);
					}
					ret2("", true);
				};
				
				const innerCompartment2 = async function(promise){
					safeQuery("LOCK TABLE WorkerTasks WRITE;", async function(){
						safeQuery(["UPDATE WorkerTasks SET Status = 2 WHERE Id = ", sqlescape(jobid), ";"].join(""), async function(){
							innerCompartment(promise);
						});
					});
				};
				
				if(res){
					innerCompartment2(BlockchainManager.sendSignedTransaction(tx));
				} else{
					safeQuery(["SELECT Status FROM WorkerTasks WHERE Id = ", sqlescape(jobid), ";"].join(""), async function(result){
						try{
							checkSafety(result.length == 1, "Corrupted task queue!");
							checkSafety(result[0].Status, "Corrupted task queue!");
						} catch{
							return;
						}
						
						if(result[0].Status == '2'){
							innerCompartment({on: async function(){}, off: async function(){}});
						} else{
							innerCompartment2(BlockchainManager.sendSignedTransaction(tx));
						}
						
					});
					
				}
				
			}
		};
		
		
		
		const method = methods[params.pop()];
		if(method){
			res = !!res;
			console.log("request parsed!");
			method();
		} else if(res){
			res.write('{"error": "Invalid request method!"}');
			res.end();
		}
	};
	http.listen(env.PORT || 80);
	
	//Start scouring the taskqueue for failed tasks
	setInterval(async function(){
		if(SQL_locked){
			return; //Don't proceed with SQL locked
		}
		useSQL(undefined, async function(fail, checkSafety, checkSafety2, safeQuery, ret2, setjobid){
			safeQuery("SELECT * FROM WorkerTasks ORDER BY Id DESC LIMIT 1;", async function(result){
				try{
					checkSafety(result.length == 1, "Corrupted pending tasks database!");
				} catch{
					return;
				}
				
				result = result[0];
				if(Date.now() > parseInt(result.LastTouched) + 900000){
					const id = parseInt(result.Id);
					setjobid(id);
					const p3 = (result.URL + result.URL2).split("/");
					p3.reverse();
					executeRequest(p3, undefined, fail, checkSafety, checkSafety2, safeQuery, ret2, id);
				}
			});
		});

	}, 1000);
	
	
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