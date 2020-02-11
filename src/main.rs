use clap::{App, Arg, SubCommand};
use colored::*;
use rusoto_core::Region;
use rusoto_sqs::{
    DeleteMessageRequest, ListQueuesRequest, ReceiveMessageRequest, SendMessageRequest, Sqs,
    SqsClient,
};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let matches = App::new("AWS SQS Replay CLI")
        .version("0.1.0")
        .about("Read messages from one queue and send them to another")
        .subcommand(
            SubCommand::with_name("send")
                .about("Send messages")
                .arg(
                    Arg::with_name("source-queue-url")
                        .short("s")
                        .long("source-queue-url")
                        .value_name("source-queue-url")
                        .help("The source SQS queue URL")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("destination-queue-url")
                        .short("d")
                        .long("destination-queue-url")
                        .value_name("destination-queue-url")
                        .help("The destination SQS queue URL")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("message-group-id")
                        .short("g")
                        .long("message-group-id")
                        .value_name("message-group-id")
                        .help("Message Group ID to use when sending to the destination queue")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .subcommand(SubCommand::with_name("list-queues").about("List SQS Queue URLs"))
        .get_matches();

    let client = SqsClient::new(Region::EuWest1);
    if let Some(matches) = matches.subcommand_matches("send") {
        let source_url = matches.value_of("source-queue-url").unwrap();
        let dest_url = matches.value_of("destination-queue-url").unwrap();
        let message_group_id = matches.value_of("message-group-id").unwrap();
        replay_messages(&client, source_url, dest_url, message_group_id);
    } else if let Some(_) = matches.subcommand_matches("list-queues") {
        list_queues(&client);
    } else {
        println!("Missing required subcommand");
        std::process::exit(1);
    }
}

fn list_queues(client: &SqsClient) {
    let list_input: ListQueuesRequest = Default::default();

    match client.list_queues(list_input).sync() {
        Ok(queues) => match queues.queue_urls {
            Some(urls) => {
                for u in urls.iter() {
                    println!("{}", u);
                }
            }
            None => println!("No queues"),
        },
        Err(error) => {
            println!("Failed to list queues: {:?}", error);
            std::process::exit(2);
        }
    }
}

fn replay_messages(client: &SqsClient, source_url: &str, dest_url: &str, message_group_id: &str) {
    println!(" {} {}", "     Source queue URL".green(), source_url);
    println!(" {} {}", "Destination queue URL".green(), dest_url);
    println!("");

    let mut more_messages = true;
    let mut batch_no = 1;

    while more_messages {
        println!(
            "{}",
            format!("Requesting {} messages in batch {}", 10, batch_no).cyan()
        );
        let receive_message_input = ReceiveMessageRequest {
            queue_url: String::from(source_url),
            max_number_of_messages: Some(10), // TODO: Extract constant
            wait_time_seconds: Some(3),
            visibility_timeout: Some(5),
            receive_request_attempt_id: None, // TODO: Should use this to request the same set of messages in the event of a failure
            ..Default::default()
        };

        match client.receive_message(receive_message_input).sync() {
            Ok(result) => match result.messages {
                Some(messages) => {
                    let count = messages.len();
                    println!("{}", format!("{} messages received\n", count).cyan());

                    if count == 0 || count < 10 {
                        more_messages = false;
                    }

                    for m in messages.iter() {
                        // println!("{:?}", m);

                        if let Some(message_id) = &m.message_id {
                            match &m.receipt_handle {
                                Some(receipt_handle) => {
                                    let body = &m.body.clone().unwrap_or(String::from("<empty>"));
                                    println!("{} {}\n{}", "Message ID".green(), message_id, body);
                                    send_message(
                                        client,
                                        dest_url,
                                        message_id.to_string(),
                                        body.to_string(),
                                        Uuid::new_v4().to_string(),
                                        String::from(message_group_id), // Pass this to the command line as required
                                    );
                                    delete_message(client, source_url, receipt_handle);
                                }
                                None => {
                                    println!("Didn't receive receipt handle for Message ID: {} Continuing to next message...", message_id);
                                }
                            }
                        }
                    }
                }
                None => {
                    println!(
                        "{}",
                        format!("No messages received in batch {}", batch_no).cyan()
                    );
                    more_messages = false;
                }
            },
            Err(error) => {
                println!("Failed to list queues: {:?}", error);
                std::process::exit(3);
            }
        }
        batch_no += 1;
    }
}

fn send_message(
    client: &SqsClient,
    dest_url: &str,
    message_id: String,
    body: String,
    dedup_id: String,
    group_id: String,
) {
    let send_message_input = SendMessageRequest {
        queue_url: String::from(dest_url),
        message_body: body,
        message_deduplication_id: Some(dedup_id),
        message_group_id: Some(group_id),
        ..Default::default()
    };

    match client.send_message(send_message_input).sync() {
        Ok(result) => {
            println!(
                "Sent successfully with sequence number {}",
                result.sequence_number.unwrap_or(String::from("<unknown>"))
            );
        }
        Err(error) => {
            println!(
                "Failed to send message ID {} to destination queue: {:?}",
                message_id, error
            );
            std::process::exit(3);
        }
    }
}

fn delete_message(client: &SqsClient, source_url: &str, receipt_handle: &String) {
    let delete_message_input = DeleteMessageRequest {
        queue_url: String::from(source_url),
        receipt_handle: receipt_handle.to_string(),
    };
    match client.delete_message(delete_message_input).sync() {
        Ok(_) => println!("Message deleted from source queue\n"),
        Err(error) => {
            println!("Failed to delete message from source queue: {:?}", error);
        }
    }
}
