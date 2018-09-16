extern crate timely;
extern crate differential_dataflow;
extern crate declarative_server;
extern crate serde_json;
extern crate mio;
extern crate slab;
extern crate ws;

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

#[macro_use]
extern crate serde_derive;

use std::collections::{HashMap};
use std::io::{Write};
use std::time::{Instant, Duration};
use std::usize;

use timely::dataflow::operators::Probe;
use timely::dataflow::operators::Map;

use mio::*;
use mio::net::{TcpListener};

use slab::Slab;

use ws::connection::{Connection, ConnEvent};

use declarative_server::{Context, Plan, Rule, TxData, Out, Datom, setup_db, register};

mod sequencer;
use sequencer::{Sequencer};

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug)]
struct Command {
    id: usize,
    // the worker (typically a controller) that issued this command
    // and is the one that should receive outputs
    owner: usize,
    cmd: String,
}

#[derive(Deserialize, Debug)]
enum Request {
    Transact { tx: usize, tx_data: Vec<TxData> },
    Register { query_name: String, plan: Plan, rules: Vec<Rule> },
    // LoadData { filename: String, max_lines: usize },
}

const SERVER: Token = Token(usize::MAX - 1);

fn main() {

    env_logger::init();

    timely::execute_from_args(std::env::args(), move |worker| {

        // setup interpreter context
        let mut ctx = worker.dataflow(|scope| {
            let (input_handle, db) = setup_db(scope);
            
            Context {
                db,
                input_handle,
                probes: Vec::new(),
                queries: HashMap::new(),
            }
        });

        // setup serialized command queue (shared between all workers)
        let mut sequencer: Sequencer<Command> = Sequencer::new(worker, Instant::now());

        // configure websocket server
        let ws_settings = ws::Settings {
            max_connections: 1024,
            .. ws::Settings::default()
        };
        
        // setup networking event polling
        let addr = "127.0.0.1:6262".parse().unwrap();
        let server = TcpListener::bind(&addr).unwrap();
        let poll = Poll::new().unwrap();
        let mut connections = Slab::with_capacity(ws_settings.max_connections);
        let mut next_connection_id: u32 = 0;
        let mut events = Events::with_capacity(1024);

        // start listening
        poll.register(&server, SERVER, Ready::readable(), PollOpt::level()).unwrap();

        info!("[WORKER {}] running on port 6262", worker.index());
        
        loop {
            
            // each worker has to...
            //
            // ...accept new client connections
            // ...accept commands on a client connection and push them to the sequencer
            // ...step computations
            // ...send results to clients
            //
            // by having everything inside a single event loop, we can
            // easily make trade-offs such as limiting the number of
            // commands consumed, in order to ensure timely progress
            // on registered queues

            std::io::stdout().flush().unwrap();
            std::io::stderr().flush().unwrap();

            println!("looping");
            
            // handle clients
            
            poll.poll(&mut events, Some(Duration::from_millis(500))).unwrap(); // @TODO handle errors

            for event in events.iter() {

                match event.token() {
                    SERVER => {
                        if event.readiness().is_readable() {
                            // new connection arrived on the server socket
                            match server.accept() {
                                Err(err) => error!("[WORKER {}] error while accepting connection {:?}", worker.index(), err),
                                Ok((socket, addr)) => {
                                    info!("[WORKER {}] new tcp connection from {}", worker.index(), addr);

                                    // @TODO to nagle or not to nagle?
                                    // sock.set_nodelay(true)

                                    let token = {
                                        let entry = connections.vacant_entry();
                                        let token = Token(entry.key());
                                        let connection_id = next_connection_id;
                                        next_connection_id = next_connection_id.wrapping_add(1);

                                        entry.insert(Connection::new(token, socket, ws_settings, connection_id));

                                        token
                                    };

                                    let conn = &mut connections[token.into()];

                                    conn.as_server().unwrap();

                                    poll.register(
                                        conn.socket(),
                                        conn.token(),
                                        conn.events(),
                                        PollOpt::edge() | PollOpt::oneshot()
                                    ).unwrap();
                                }
                            }
                        }
                    },
                    _ => {
                        let token = event.token();
                        let active = {
                            let readiness = event.readiness();
                            let conn_events = connections[token.into()].events();

                            // @TODO refactor connection to accept a
                            // vector in which to place events and
                            // rename conn_events to avoid name clash
                            
                            if (readiness & conn_events).is_readable() {
                                match connections[token.into()].read() {
                                    Err(err) => {
                                        trace!("[WORKER {}] error while reading: {}", worker.index(), err);
                                        // @TODO error handling
                                        connections[token.into()].error(err)
                                    },
                                    Ok(mut conn_events) => {
                                        for conn_event in conn_events.drain(0..) {
                                            match conn_event {
                                                ConnEvent::Message(msg) => {
                                                    let command = Command {
                                                        id: 0, // @TODO command ids?
                                                        owner: worker.index(),
                                                        cmd: msg.into_text().unwrap()
                                                    };

                                                    trace!("[WORKER {}] {:?}", worker.index(), command);
                                                    
                                                    sequencer.push(command);
                                                },
                                                _ => { println!("other"); }
                                            }
                                        }
                                    }
                                }
                            }

                            let conn_events = connections[token.into()].events();

                            if (readiness & conn_events).is_writable() {
                                match connections[token.into()].write() {
                                    Err(err) => {
                                        trace!("[WORKER {}] error while writing: {}", worker.index(), err);
                                        // @TODO error handling
                                        connections[token.into()].error(err)
                                    },
                                    Ok(_) => { }
                                }
                            }

                            // connection events may have changed
                            connections[token.into()].events().is_readable()
                                || connections[token.into()].events().is_writable()
                        };

                        // NOTE: Closing state only applies after a ws connection was successfully
                        // established. It's possible that we may go inactive while in a connecting
                        // state if the handshake fails.
                        if !active {
                            if let Ok(addr) = connections[token.into()].socket().peer_addr() {
                                debug!("WebSocket connection to {} disconnected.", addr);
                            } else {
                                trace!("WebSocket connection to token={:?} disconnected.", token);
                            }
                            connections.remove(token.into());
                        } else {
                            let conn = &connections[token.into()];
                            poll.reregister(
                                conn.socket(),
                                conn.token(),
                                conn.events(),
                                PollOpt::edge() | PollOpt::oneshot(),
                            ).unwrap();
                        }
                    }
                }
            }

            // handle commands
                        
            while let Some(command) = sequencer.next() {

                match serde_json::from_str::<Request>(&command.cmd) {
                    Err(msg) => { error!("failed to parse command: {:?}", msg); },
                    Ok(req) => {

                        info!("[WORKER {}] {:?}", worker.index(), req);
                        
                        match req {
                            Request::Transact { tx, tx_data } => {
                                
                                for TxData(op, e, a, v) in tx_data {
                                    ctx.input_handle.update(Datom(e, a, v), op);
                                }
                                
                                ctx.input_handle.advance_to(tx + 1);
                                ctx.input_handle.flush();
                            },
                            Request::Register { query_name, plan, rules } => {

                                worker.dataflow::<usize, _, _>(|scope| {
                                    let mut rel_map = register(scope, &mut ctx, &query_name, plan, rules);

                                    let probe = rel_map.get_mut(&query_name).unwrap().trace.import(scope)
                                        .as_collection(|tuple,_| tuple.clone())
                                        .inner
                                        .map(|x| Out(x.0.clone(), x.2))
                                        .unary_notify(
                                            timely::dataflow::channels::pact::Exchange::new(move |_: &Out| command.owner as u64), 
                                            "OutputsRecv", 
                                            Vec::new(),
                                            move |input, _output: &mut OutputHandle<_, Out, _>, _notificator| {

                                                input.for_each(|_time, data| {
                                                    // @TODO send outputs
                                                    // let out: Vec<Out> = data.drain(..).collect();
                                                    // send_results.send((query_name.clone(), out));
                                                });
                                            })
                                        .probe();

                                    ctx.probes.push(probe);
                                });
                            }
                        }
                    }
                }
            }

            if ctx.probes.is_empty() {
                // ensure loop continues while no queries registered
                worker.step();
            } else {
                for probe in &mut ctx.probes {
                    while probe.less_than(ctx.input_handle.time()) {
                        worker.step();
                    }
                }
            }
        }

        info!("[WORKER {}] exited command loop", worker.index());
        
    }).unwrap(); // asserts error-free execution
}

// output for experiments
// let probe = rel_map.get_mut(&query_name).unwrap().trace.import(scope)
//     .as_collection(|_,_| ())
//     .consolidate()
//     .inspect(move |x| println!("Nodes: {:?} (at {:?})", x.2, ::std::time::Instant::now()))
//     .probe();

// fn handle_load_data(filename: String, max_lines: usize) {
//     let load_timer = ::std::time::Instant::now();
//     let peers = worker.peers();
//     let file = BufReader::new(File::open(filename).unwrap());
//     let mut line_count: usize = 0;

//     let attr_node: Attribute = 100;
//     let attr_edge: Attribute = 200;

//     for readline in file.lines() {
//         let line = readline.ok().expect("read error");

//         if line_count > max_lines { break; };
//         line_count += 1;
        
//         if !line.starts_with('#') && line.len() > 0 {
//             let mut elts = line[..].split_whitespace();
//             let src: u64 = elts.next().unwrap().parse().ok().expect("malformed src");
            
//             if (src as usize) % peers == index {
//                 let dst: u64 = elts.next().unwrap().parse().ok().expect("malformed dst");
//                 let typ: &str = elts.next().unwrap();
//                 match typ {
//                     "n" => { ctx.input_handle.update(Datom(src, attr_node, Value::Eid(dst)), 1); },
//                     "e" => { ctx.input_handle.update(Datom(src, attr_edge, Value::Eid(dst)), 1); },
//                     unk => { panic!("unknown type: {}", unk)},
//                 }
//             }
//         }
//     }

//     if index == 0 {
//         println!("{:?}:\tData loaded", load_timer.elapsed());
//         println!("{:?}", ::std::time::Instant::now());
//     }
    
//     next_tx = next_tx + 1;
//     ctx.input_handle.advance_to(next_tx);
//     ctx.input_handle.flush();
// }

// /// The controller is a differential dataflow serializing and
// /// circulating new commands to all workers.
// fn build_controller<A: timely::Allocate>(
//     worker: &mut Root<A>,
//     timer: ::std::time::Instant,
//     input_recv: Weak<Mutex<Receiver<String>>>,
//     command_queue: &Rc<RefCell<VecDeque<Command>>>,
//     handle: &mut ProbeHandle<Product<RootTimestamp, usize>>,
// ) {

//     let this_idx = worker.index();
//     let command_queue = command_queue.clone();

//     // command serialization and circulation
//     worker.dataflow(move |dataflow| {

//         let peers = dataflow.peers();
//         let mut recvd_commands = Vec::new();

//         // source attempting to pull from input_recv and producing
//         // commands for everyone
//         source(dataflow, "InputCommands", move |capability| {

//             // so we can drop, if input queue vanishes.
//             let mut capability = Some(capability);

//             // closure broadcasts any commands it grabs.
//             move |output| {

//                 if let Some(input_recv) = input_recv.upgrade() {

//                     // determine current nanoseconds
//                     if let Some(capability) = capability.as_mut() {

//                         // this could be less frequent if needed.
//                         let mut time = capability.time().clone();
//                         let elapsed = timer.elapsed();
//                         time.inner = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as usize;

//                         // downgrade the capability.
//                         capability.downgrade(&time);

//                         if let Ok(input_recv) = input_recv.try_lock() {
//                             while let Ok(cmd) = input_recv.try_recv() {
//                                 let mut session = output.session(&capability);
//                                 for worker_idx in 0 .. peers {
//                                     // @TODO command ids?
//                                     session.give((worker_idx, Command { id: 0, owner: this_idx, cmd: cmd.clone() }));
//                                 }
//                             }
//                         }
//                     } else { panic!("command serializer: capability lost while input queue valid"); }
//                 } else { capability = None; }
//             }
//         })
//         .unary_notify(
//             timely::dataflow::channels::pact::Exchange::new(|x: &(usize, Command)| x.0 as u64), 
//             "InputCommandsRecv", 
//             Vec::new(), 
//             move |input, output, notificator| {

//             // grab all commands
//             input.for_each(|time, data| {
//                 recvd_commands.extend(data.drain(..).map(|(_,command)| (time.time().clone(), command)));
//                 if false { output.session(&time).give(0u64); }
//             });

//             recvd_commands.sort();

//             // try to move any commands at completed times to a shared queue.
//             if let Ok(mut borrow) = command_queue.try_borrow_mut() {
//                 while recvd_commands.len() > 0 && !notificator.frontier(0).iter().any(|x| x.less_than(&recvd_commands[0].0)) {
//                     borrow.push_back(recvd_commands.remove(0).1);
//                 }
//             } else { panic!("failed to borrow shared command queue"); }
//         })
//         .probe_with(handle);
//     });
// }

// fn run_cli_server(command_channel: Sender<String>, results_channel: Receiver<(String, Vec<Out>)>) {

//     stdout().flush().unwrap();
//     let input = stdin();

//     Command { id: 0, owner: self.worker_idx, cmd: input }


//     thread::spawn(move || {
//         loop {
//             match results_channel.recv() {
//                 Err(_err) => break,
//                 Ok((query_name, results)) => { println!("=> {:?} {:?}", query_name, results) }
//             };
//         }
//     });
    
//     loop {
//         if let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
//             match line.as_str() {
//                 "exit" => { break },
//                 _ => { command_channel.send(line).expect("failed to send command"); },
//             }
//         }
//     }
// }

// struct WsServer {
//     out: Sender,
//     command_channel: Sender<String>,
//     results_channel: Receiver<(String, Vec<Out>)>
// }

// impl WsServer {

//     fn on_results(&mut self, query_name: &str, results: &Vec<Out>) {
//         let serialized = serde_json::to_string::<(String, Vec<Out>)>(&(query_name, results))
//             .expect("failed to serialize outputs");
        
//         self.out.send(serialized).expect("failed to send message");
//     }
// }

// impl Handler for WsServer {

//     fn on_open(&mut self, _: Handshake) -> Result<()> {
//         println!("[WS-SERVER] new connection");
//     }
    
//     fn on_message(&mut self, msg: Message) -> Result<()> {
//         command_channel.send(line).expect("failed to send command");
//     }

//     fn on_close(&mut self, code: CloseCode, reason: &str) {
//         println!("[WS-SERVER] client closed connection {:?} {:?}", code, reason);
//         self.out.shutdown().unwrap();
//     }
// }

// fn run_ws_server(command_channel: Sender<String>, results_channel: Receiver<(String, Vec<Out>)>) {

//     listen("127.0.0.1:6262", |out| {
//         WsServer {
//             out: out,
//             command_channel: command_channel.clone(),
//             results_channel: results_channel.clone(),
//         }
//     });

//     println!("[WS-SERVER] running on port 6262");

//     for connection in server.filter_map(Result::ok) {

//         let send_handle = command_channel.clone();
        
//         thread::spawn(move || {
// 			      let client = connection.accept().expect("[WS-SERVER] failed to accept");

//             println!("[WS-SERVER] connection from {:?}", client.peer_addr().unwrap());

// 			      let (mut receiver, mut sender) = client.split().unwrap();

//             loop {
//                 match receiver.recv() {
//                     Err(_err) => { },
//                     Ok(msg) => {
//                         match msg {
//                             OwnedMessage::Close(_) => { println!("[WS-SERVER] client closed connection"); },
//                             OwnedMessage::Text(line) => { send_handle.send(line).expect("failed to send command"); },
//                             _ => { },
//                         }
//                     }
//                 }
                    
//                 match results_channel.try_recv() {
//                     Err(_err) => break,
//                     Ok((query_name, results)) => {
//                         let serialized = serde_json::to_string::<(String, Vec<Out>)>(&(query_name, results)).expect("failed to serialize outputs");
//                         sender.send_message(&Message::text(serialized)).expect("failed to send message");
//                     }
//                 };
//             }
//         });
//     }

//     println!("[WS-SERVER] exited");
// }

// fn run_tcp_server(command_channel: Sender<String>, results_channel: Receiver<(String, Vec<Out>)>) {

//     let send_handle = &command_channel;
    
//     let listener = TcpListener::bind("127.0.0.1:6262").expect("can't bind to port");
//     listener.set_nonblocking(false).expect("Cannot set blocking");

//     println!("[TCP-SERVER] running on port 6262");
    
//     match listener.accept() {
//         Ok((stream, _addr)) => {
            
//             println!("[TCP-SERVER] accepted connection");

//             let mut out_stream = stream.try_clone().unwrap();
//             let mut writer = BufWriter::new(out_stream);
            
//             thread::spawn(move || {
//                 loop {
//                     match results_channel.recv() {
//                         Err(_err) => break,
//                         Ok(results) => {
//                             let serialized = serde_json::to_string::<(String, Vec<Out>)>(&results)
//                                 .expect("failed to serialize outputs");
                            
//                             writer.write(serialized.as_bytes()).expect("failed to send output");
//                         }
//                     };
//                 }
//             });
            
//             let mut reader = BufReader::new(stream);
//             for input in reader.lines() {
//                 match input {
//                     Err(e) => { println!("Error reading line {}", e); break; },
//                     Ok(line) => {
//                         println!("[TCP-SERVER] new message: {:?}", line);
                        
//                         send_handle.send(line).expect("failed to send command");
//                     }
//                 }
//             }

//             println!("[TCP-SERVER] closing connection");
//         },
//         Err(e) => { println!("Encountered I/O error: {}", e); }
//     }
    
//     println!("[TCP-SERVER] exited");
// }
