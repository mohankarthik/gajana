import { Injectable, Logger } from '@nestjs/common';
import fs = require('fs');
import { OAuth2Client, Credentials } from 'google-auth-library';
import { gmail_v1, google } from 'googleapis';
import path = require('path');

export interface GoogleCredentialInstalled {
  client_id: string;
  project_id: string;
  auth_uri: string;
  token_uri: string;
  auth_provider_x509_cert_url: string;
  client_secret: string;
  redirect_uris: string[];
}

export interface GoogleCredential {
  installed: GoogleCredentialInstalled;
}

@Injectable()
export class DataAcquisitionGmailService {
  private logger = new Logger(DataAcquisitionGmailService.name);
  private TOKEN_PATH = 'apps/api/src/assets/credentials';
  private oAuthClients: OAuth2Client[] = [];
  private gmailClients: gmail_v1.Gmail[] = [];

  constructor() {
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
  private _authorize() {
    // Check if we have previously stored a token.
    fs.readdir(this.TOKEN_PATH, (err, files) => {
      if (err) {
        this.logger.error(err);
        throw err;
      }

      files.forEach((file) => {
        if (file.search('token') !== -1) {
          fs.readFile(path.join(this.TOKEN_PATH, file), (err, token) => {
            if (err) {
              this.logger.error(err);
              throw err;
            }
            const oAuth2Client = new google.auth.OAuth2(
              process.env.GOOGLE_CLIENT_ID,
              process.env.GOOGLE_CLIENT_SECRET,
              process.env.GOOGLE_REDIRECT_URI
            );
            // const credentials: Credentials = {
            //   refresh_token: '',
            //   expiry_date: 0,
            //   access_token: '',
            //   token_type: '',
            //   id_token: '',
            //   scope: "https://www.googleapis.com/auth/gmail.readonly"
            // };
            oAuth2Client.setCredentials(JSON.parse(token.toString()));

            const gmail = google.gmail({ version: 'v1', auth: oAuth2Client });
            this.gmailClients.push(gmail);
            this.oAuthClients.push(oAuth2Client);
            this.logger.log(`Setting Google credentials from file ${file}`);
          });
        }
      });
    });
  }
}
