import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import { SQSHandler } from "aws-lambda";

let sesClient: SESClient | null = null;

const getSESClient = (): SESClient => {
    if (!sesClient) {
        sesClient = new SESClient({});
    }
    return sesClient;
};

export const handler: SQSHandler = async (event) => {
    console.log("SQSからのイベント:", JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            const body = JSON.parse(record.body);
            const payload = body.responsePayload || body;
            const reportContent = payload.report_content;
            const reportKey = payload.report_key;

            if (!reportContent) {
                console.error("Report contentがありません");
                continue;
            }

            const senderEmail = process.env.SENDER_EMAIL;
            const destinationEmail = process.env.DESTINATION_EMAIL;

            if (!senderEmail || !destinationEmail) {
                console.error("SENDER_EMAIL か DESTINATION_EMAIL が設定されていません");
                continue;
            }

            const command = new SendEmailCommand({
                Destination: {
                    ToAddresses: [destinationEmail],
                },
                Message: {
                    Body: {
                        Text: {
                            Charset: "UTF-8",
                            Data: reportContent,
                        },
                    },
                    Subject: {
                        Charset: "UTF-8",
                        Data: `【売上レポート】${reportKey}`,
                    },
                },
                Source: senderEmail,
            });

            const response = await getSESClient().send(command);
            console.log("Emailの送信に成功しました:", response.MessageId);
        } catch (error) {
            console.error("SQS recordの処理でエラーが発生しました:", error);
            throw error;
        }
    }
};
