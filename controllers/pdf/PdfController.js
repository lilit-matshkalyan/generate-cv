const amqp = require('amqplib/callback_api');
const fs = require('fs');
const pdfPrinter = require('pdfmake');
const zipdir = require('zip-dir');

const dir = `./cv-${new Date()}`;
let amqpConn = null;

const userData = require('../../users.json')
let messageCount = userData.length;

let pubChannel = null;
const routingKey = 'jobs';
const offlinePubQueue = [];
const CLOUDAMQP_URL = 'amqps://qfyxiczf:aAd8jUzG8sP67aGt8cDHDyru88iQt8Dk@shrimp.rmq.cloudamqp.com/qfyxiczf';

let fonts = {
    Roboto: {
        normal: 'fonts/Roboto-Regular.ttf',
        bold: 'fonts/Roboto-Medium.ttf',
        italics: 'fonts/Roboto-Italic.ttf',
        bolditalics: 'fonts/Roboto-MediumItalic.ttf'
    }
};

const printer = new pdfPrinter(fonts);


class PdfController {
    static start() {
        amqp.connect(CLOUDAMQP_URL + "?heartbeat=60", function(err, conn) {
            if (err) {
                console.error("[AMQP]", err.message);
                return setTimeout(PdfController.start, 1000);
            }
            conn.on("error", function(err) {
                if (err.message !== "Connection closing") {
                    console.error("[AMQP] conn error", err.message);
                }
            });
            conn.on("close", function() {
                console.error("[AMQP] reconnecting");
                return setTimeout(PdfController.start, 1000);
            });
            console.log("[AMQP] connected");
            amqpConn = conn;
            PdfController.whenConnected();
        });
    }

    static whenConnected() {
        PdfController.startPublisher();
        PdfController.startWorker();
    }

    static startPublisher() {
        amqpConn.createConfirmChannel(function(err, ch) {
            if (PdfController.closeOnErr(err)) return;
            ch.on("error", function(err) {
                console.error("[AMQP] channel error", err.message);
            });
            ch.on("close", function() {
                console.log("[AMQP] channel closed");
            });

            pubChannel = ch;
            while (true) {
                const m = offlinePubQueue.shift();

                if (!m) break;

                PdfController.publish(m[0], m[1], m[2]);
            }
        });
    }

    static publish(exchange, routingKey, content) {
        try {
            pubChannel.publish(exchange, routingKey, content, { persistent: true },
                function(err, ok) {
                    if (err) {
                        console.error("[AMQP] publish", err);
                        offlinePubQueue.push([exchange, routingKey, content]);
                        pubChannel.connection.close();
                    }
                });
        } catch (e) {
            console.error("[AMQP] publish", e.message);
            offlinePubQueue.push([exchange, routingKey, content]);
        }
    }

    static startWorker() {
        amqpConn.createChannel(function(err, ch) {
            if (PdfController.closeOnErr(err)) return;
            ch.on("error", function(err) {
                console.error("[AMQP] channel error", err.message);
            });

            ch.on("close", function() {
                console.log("[AMQP] channel closed");
            });

            ch.prefetch(10);
            ch.assertQueue(routingKey, { durable: true }, function(err, _ok) {
                if (PdfController.closeOnErr(err)) return;
                ch.consume(routingKey, processMsg, { noAck: false });
                console.log("Worker is started");
            });

            function processMsg(msg) {
                PdfController.work(msg, function(ok) {
                    try {
                        if (ok)
                            ch.ack(msg);
                        else
                            ch.reject(msg, true);
                    } catch (e) {
                        PdfController.closeOnErr(e);
                    }
                });
            }
        });
    }

    static work(msg, cb) {
        console.log(`PDF processing of user with ${JSON.parse(msg.content).email} this email`);
        PdfController.generatePDF(JSON.parse(msg.content));
        --messageCount
        if (messageCount === 0) {
            PdfController.zipCV();
            PdfController.removeDirectory();
        }
        cb(true);
    }

    static closeOnErr(err) {
        if (!err) return false;
        console.error("[AMQP] error", err);
        amqpConn.close();
        return true;
    }

    static generatePDF(user) {
        const docDefinition = {
            content: [
                `email: ${user.email}`,
                `gender: ${user.gender}`,
                `phone_number: ${user.phone_number}`,
                `birthdate: ${user.birthdate}`,
                `street: ${user.location.street}`,
                `city: ${user.location.city}`,
                `state: ${user.location.state}`,
                `postcode: ${user.location.postcode}`,
                `username: ${user.username}`,
                `password: ${user.password}`,
                `first_name: ${user.first_name}`,
                `last_name: ${user.last_name}`,
                `title: ${user.title}`,
                `picture: ${user.picture}`,
            ]
        };

        const pdfDoc = printer.createPdfKitDocument(docDefinition);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        pdfDoc.pipe(fs.createWriteStream(`${dir}/${user.email}.pdf`));
        pdfDoc.end();
    }

    static zipCV() {
        zipdir(dir, {saveTo: `${dir}-${new Date()}.zip`}, function (err, buffer) {
            console.log(err)
        });
    }

    static removeDirectory() {
        fs.rmdir(dir, { recursive: true }, (err) => {
            if (err) {
                throw err;
            }
            console.log(`${dir} is deleted!`);
        });
    }
}

module.exports = PdfController;
