import Foundation
import SwiftKafka

let arguments: [String] = Array(CommandLine.arguments.dropFirst())

guard let producer: String = arguments.first else {
    exit(EXIT_FAILURE)
}

struct Message : Codable {
    var category: Int
    var value: String
    
    init(category:Int, counter:Int) {
        self.category=category
        value = "abcd-\(counter)"
    }
}

class Encoder {
    static let jsonEncoder = JSONEncoder()
    
    func encode<T:Codable>(_ value:T) -> String {
        let jsonData = try! Encoder.jsonEncoder.encode(value)
        return String(data: jsonData, encoding: String.Encoding.utf8)!
    }
}

let encoder = Encoder()

if(producer == "producer") {
    print("Starting producing")
    let formatter = DateFormatter()
    var counter = 1
    var timer = Date()
    let config = KafkaConfig()
    config["partition.assignment.strategy"] = "roundrobin"
    let producer = try KafkaProducer(config: config)
    guard producer.connect(brokers: "192.168.1.33:9092") == 1 else {
        throw KafkaError(rawValue: 8)
    }
    while(true) {
        //        print("Sending \(counter)")
        autoreleasepool {
            let message = Message(category: 1, counter: counter)
            
            producer.send(producerRecord: KafkaProducerRecord(topic: "topic3", value: encoder.encode(message), key: encoder.encode("\(counter)"))) {result in return}
        }
//        { result in
//            switch result {
//            case .success(let message):
//                return
//            //                print("Message at offset \(message.offset) successfully sent")
//            case .failure(let error):
//                print("Error producing: \(error)")
//            }
//        }
        //        sleep(5)
        if(counter % 100000 == 0) {
            if #available(OSX 10.15, *) {
                print("Sent \(counter) at \(formatter.string(from: Date())) in \(timer.distance(to: Date()))s")
            } else {
                // Fallback on earlier versions
            }
            timer = Date()
            //            print("Sent \(counter)")
        }
        counter += 1
        
    }
}

//let consumer = cluster.getConsumer(topics: ["test"], groupId: "1")
print("Consumer")
//consumer.listen { message in
//    print(String(data: message.value, encoding: .utf8)!)
//}
