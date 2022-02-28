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
	const unlockSQL_sync = function(){
		//If there is nothing waiting to use SQL, unlock!
		//otherwise, execute next SQL-using task without releasing and reacquiring lock!
		const shift = SQL_queue.shift();
		if(shift){
			shift();
		} else{
			SQL_locked = false;
		}	
	};
	const unlockSQL = async function(){
		unlockSQL_sync();
	};
	
	let safe_ungraceful_exit = true;
	
	//NOTE: f should not be an asynchronous function.
	const useSQL = async function(response, f){
		awaitSQL(async function(){
			let jobid = undefined;
			//Core safety methods
			
			let connection_open = !!response; //If we are in connectionless mode, then this would come in handy!
			let _tempfuncexport;
			{
				const res2 = response;
				_tempfuncexport = function(msg){
					sql.query("ROLLBACK;", async function(){
						//Remove from task queue if possible
						safe_ungraceful_exit = false;
						sql.query(["DELETE FROM WorkerTasks WHERE Id = ", sqlescape(jobid), ";"].join(""), async function(){
							safe_ungraceful_exit = true;
							unlockSQL_sync();
						});
					});
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
						safe_ungraceful_exit = true;
						if(connection_open){
							res2.write(JSON.stringify({returns: data}));
							res2.end();
							connection_open = false;
						}						
					};
					const handle2 = async function(err){
						try{
							checkSafety2(err, "Unable to update task status!");	
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
							checkSafety2(err, "Unable to unlock tables!");	
						} catch {
							return;
						}
						
						if(jobid){
							sql.query(["DELETE FROM WorkerTasks WHERE Id = ", sqlescape(jobid), ";"].join(""), handle2);
						} else{
							handle2(false);
						}
						
						
					};
					if(nocommit){
						handle3(false);
					} else{
						safe_ungraceful_exit = false;
						sql.query("UNLOCK TABLES;", handle);
					}
				};
			}
			response = undefined;
			const safeQuery = async function(query, callback){
				sql.query(query, async function(err, res){
					try{
						checkSafety2(err, "SQL Query returned error: " + query);
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
	
	let parallelCreditLoop;
	const parallelCreditQueue = [];
	{
		parallelCreditLoop = setInterval(async function(){
			//Low-priority lock
			if(SQL_locked || beforesqlavail || parallelCreditQueue.length == 0){
				return;
			} else {
				SQL_locked = true;
				sql.query("START TRANSACTION;", async function(err){
					if(err){
						unlockSQL();
						return;
					}

					
					sql.query("LOCK TABLES Balances WRITE;", async function(err){
						if(err){
							unlockSQL();
							return;
						}
						
						const lockqueue = [];
						
						const callback2 = async function(){
							let current = parallelCreditQueue.pop();
							if(current){
								//Append next cycle to event loop
								callback2();
							} else{
								//Ran out of messages (not error)
								sql.query("UNLOCK TABLES;", unlockSQL);
								return;
							}
							
							//NOTE: WE DO NOT UNLOCK SQL after this bit!
							const amount = current[0];
							const selector = [" WHERE Coin = ", sqlescape(current[1]), " AND UserID = ", sqlescape(current[2]), ";"].join("");
							const res = current[3];
							const hash = [current[2], current[1]].join("_");
							current = undefined;
							
							const fail = function(){
								res.write("error");
								res.end();
								throw "";
							};
							const safe_assert_true = function(v){
								if(!v){
									fail();
								}
							};
							const safe_assert_false = function(v){
								if(v){
									fail();
								}
							};
							
							const dowork = async function(){
								sql.query(["SELECT Balance FROM Balances", selector].join(""), async function(error, result){
									let balance = undefined;
									try{
										safe_assert_false(error);
										safe_assert_true(result);
										safe_assert_true(result.length == 1);
										safe_assert_true(result[0]);
										safe_assert_true(result[0].Balance);
										try{
											balance = sqlescape((new BigNumber(result[0].Balance)).add(new BigNumber(amount)).toString());
										} catch{
											fail();
										}
									} catch{
										return;
									}
									sql.query(["UPDATE Balances SET Balance = ", balance, selector].join(""), async function(err){
										res.write(err ? "error" : "ok");
										res.end();
										const next = lockqueue[hash].pop();
										if(next){
											next();
										}
									});
								});
							};
							
							if(lockqueue[hash]){
								lockqueue[hash].push(dowork);
							} else{
								lockqueue[hash] = [];
								dowork();
							}
							
						};
						callback2();

					});
				});
			}
		}, 100);
	}
	
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
		const nosql_methods = ["parallelCredit"];
		if(nosql_methods.indexOf(params[params.length - 1]) > -1){
			//Execute request without SQL
			//NOTE: If we run into trouble, just return. We have no SQL transactions to revert.
			const methods = {
				parallelCredit: async function(){
					if(params.length != 3){
						res.write("error");
						res.end();
						return;
					}
					//to/coin/amount
					parallelCreditQueue.push([params[0], params[1], params[2], res]);
				}
			};
			
			//Already checked
			methods[params.pop()]();
			return;
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
			chains.polygon = new eth('https://polygon-rpc.com');
			chains.mintme = new eth('https://node1.mintme.com:443');
		}
		let jobAborted = false;
		const abort = async function(){
			jobAborted = true;
		};
		let jobTimeout = setTimeout(abort, 600000);
		let failures = 0;
		const softfail = function(){
			if(++failures == 5){
				jobAborted = true;
				try{
					fail("");
				} catch{
					return;
				}
			}
		};
		const methods = {
			sendAndCreditWhenSecure: async function(){
				
				//auth/method/chain/tx/account/token/amount
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
						if(n < 10 || lock2 || !receipt){
							return;
						}
						lock2 = true;
						promise.off('confirmation', confirmation);
						if(receipt.status){
							const selector = [" WHERE Coin = ", sqlescape(token), " AND UserID = ", sqlescape(account), ";"].join("");
							safeQuery("LOCK TABLE Balances WRITE;", async function(){
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
							if(jobAborted || lock2){
								clearInterval(interval);
							} else{
								BlockchainManager.getTransactionReceipt(hash, async function(error, receipt){
									if(!receipt){
										softfail();
										return;
									}
									
									if(!receipt.blockNumber){
										softfail();
										return;
									}
									
									BlockchainManager.getBlockNumber(async function(error, blocknumber2){
										if(blocknumber2){
											confirmation(blocknumber2 - receipt.blockNumber, receipt);
										} else{
											softfail();
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
					safeQuery(["UPDATE WorkerTasks SET LastTouched = ", Date.now().toString(), ", Status = 2 WHERE Id = ", sqlescape(jobid), ";"].join(""), async function(){
						safeQuery("COMMIT;", async function(){
							clearTimeout(jobTimeout);
							jobTimeout = setTimeout(abort, 600000);
							safeQuery("START TRANSACTION;", async function(){
								innerCompartment(promise);
							});
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
				if(result.length == 0){
					ret2("");
					return;
				}
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
		
		const executeExit = async function(){
			if(beforesqlavail || safe_ungraceful_exit){
				//It's safe to abort ungracefully here
				process.exit();
			} else{
				//No SQL jobs should be running once we got this lock!
				if(SQL_locked){
					SQL_queue[0] = process.exit;
				} else{
					process.exit();
				}
				
			}
		};
		
		if(http){
			http.close();
			http = null;
			if(parallelCreditLoop){
				parallelCreditLoop = undefined;	
				parallelCreditQueue.length = 0;
				setTimeout(executeExit, 5000);
			} else{
				executeExit();
			}
		}
		
		
	});
}