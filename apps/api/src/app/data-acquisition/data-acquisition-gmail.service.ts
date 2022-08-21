import { Injectable, Logger } from '@nestjs/common';
import { OAuth2Client } from 'google-auth-library';
import { gmail_v1, google } from 'googleapis';
import { GoogleTokenService } from './google-token-service';

@Injectable()
export class DataAcquisitionGmailService {
  private logger = new Logger(DataAcquisitionGmailService.name);
  private oAuthClients: OAuth2Client[] = [];
  private gmailClients: gmail_v1.Gmail[] = [];

  constructor(private readonly googleTokenService: GoogleTokenService) {
    // Load the credentials
    this.logger.log(`Setting Google base credentials`);

    // Authorize a client with credentials, then call the Gmail API.
    this._authorize();
  }

  getLabels(): Promise<string[]> {
    return new Promise((resolve, reject) => {
      this.gmailClients.forEach((client) => {
        client.users.labels.list(
          {
            userId: 'me',
          },
          (err, res) => {
            if (err) {
              console.error(err);
              reject(err);
            }
            resolve(res.data.labels.map((x) => x.name));
          }
        );
      });
    });
  }

  /**
   * Create an OAuth2 client with the configured credentials
   */
  private async _authorize() {
    // Check if we have previously stored a token.
    const gmailTokens = await this.googleTokenService.findAll();
    for (let i = 0; i < gmailTokens.length; i++) {
      const oAuth2Client = new google.auth.OAuth2(
        process.env.GOOGLE_CLIENT_ID,
        process.env.GOOGLE_CLIENT_SECRET,
        process.env.GOOGLE_REDIRECT_URI
      );

      oAuth2Client.setCredentials(gmailTokens[i]);

      const gmail = google.gmail({ version: 'v1', auth: oAuth2Client });
      this.gmailClients.push(gmail);
      this.oAuthClients.push(oAuth2Client);
      this.logger.log(`Setting Google credentials`);
    }
  }
}
