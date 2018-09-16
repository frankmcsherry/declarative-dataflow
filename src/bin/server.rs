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
use std::time::{Instant, Duration};
use std::usize;

use timely::dataflow::operators::{Probe, Map, Operator};
use timely::dataflow::operators::generic::{OutputHandle};

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
    // the client token that issued the command (only relevant to the
    // owning worker, no one else has the connection)
    client: usize,
    cmd: String,
}

#[derive(Deserialize, Debug)]
enum Request {
    Transact { tx: usize, tx_data: Vec<TxData> },
    Register { query_name: String, plan: Plan, rules: Vec<Rule> },
    // LoadData { filename: String, max_lines: usize },
}

const SERVER: Token = Token(usize::MAX - 1);
const RESULTS: Token = Token(usize::MAX - 2);

fn main() {

    env_logger::init();

    timely::execute_from_args(std::env::args(), move |worker| {

        // setup interpreter context
        let mut ctx = worker.dataflow(|scope| {
            let (input_handle, db) = setup_db(scope);

            Context { db, input_handle, queries: HashMap::new(), }
        });
        let mut probes = Vec::new();

        // mapping from query names to interested client tokens
        let mut interests: HashMap<String, Vec<Token>> = HashMap::new();

        // setup serialized command queue (shared between all workers)
        let mut sequencer: Sequencer<Command> = Sequencer::new(worker, Instant::now());

        // configure websocket server
        let ws_settings = ws::Settings {
            max_connections: 1024,
            .. ws::Settings::default()
        };

        // setup results channel
        let (send_results, recv_results) = mio::channel::channel();
        
        // setup server socket
        let addr = "127.0.0.1:6262".parse().unwrap();
        let server = TcpListener::bind(&addr).unwrap();
        let mut connections = Slab::with_capacity(ws_settings.max_connections);
        let mut next_connection_id: u32 = 0;

        // setup event loop
        let poll = Poll::new().unwrap();
        let mut events = Events::with_capacity(1024);

        poll.register(&recv_results, RESULTS, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
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

            trace!("looping");

            // handle clients

            // @TODO 0 timeout (but can use this for artificial braking)
            // @TODO handle errors
            poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();

            for event in events.iter() {

                trace!("[WORKER {}] recv event on {:?}", worker.index(), event.token());
                
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
                    RESULTS => {
                        while let Ok((query_name, results)) = recv_results.try_recv() {

                            info!("[WORKER {}] {:?} {:?}", worker.index(), query_name, results);

                            match interests.get(&query_name) {
                                None => { /* @TODO unregister this flow */ },
                                Some(tokens) => {
                                    let serialized = serde_json::to_string::<(String, Vec<Out>)>(&(query_name, results))
                                        .expect("failed to serialize outputs");
                                    let msg = ws::Message::text(serialized);

                                    for &token in tokens.iter() {
                                        // @TODO check whether connection still exists
                                        let conn = &mut connections[token.into()];
                                        info!("[WORKER {}] sending msg {:?}", worker.index(), msg);

                                        conn.send_message(msg.clone()).expect("failed to send message");

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

                        poll.reregister(
                            &recv_results,
                            RESULTS,
                            Ready::readable(),
                            PollOpt::edge() | PollOpt::oneshot()
                        ).unwrap();
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
                                                        client: token.into(),
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
                    Err(msg) => { panic!("failed to parse command: {:?}", msg); },
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

                                if command.owner == worker.index() {

                                    // we are the owning worker and thus have to
                                    // keep track of this client's new interest
                                    
                                    let client_token = Token(command.client);
                                    interests.entry(query_name.clone())
                                        .or_insert(Vec::new())
                                        .push(client_token);
                                }

                                let send_results_handle = send_results.clone();

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
                                                
                                                // due to the exchange pact, this closure is only
                                                // executed by the owning worker

                                                input.for_each(|_time, data| {
                                                    let out: Vec<Out> = data.to_vec();
                                                    send_results_handle.send((query_name.clone(), out)).unwrap();
                                                });
                                            })
                                        .probe();

                                    probes.push(probe);
                                });
                            }
                        }
                    }
                }
            }

            // ensure work continues, even if no queries registered,
            // s.t. the sequencer continues issuing commands
            worker.step();
            
            for probe in &mut probes {
                while probe.less_than(ctx.input_handle.time()) {
                    worker.step();
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
