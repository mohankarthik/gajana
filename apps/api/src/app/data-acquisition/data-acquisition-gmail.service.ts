import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { OAuth2Client } from 'google-auth-library';
import { gmail_v1, google } from 'googleapis';
import { GaxiosResponse } from 'googleapis-common';
import moment = require('moment');
import { merge, Observable, combineLatest, forkJoin, from } from 'rxjs';
import {
  map,
  mergeAll,
  mergeMap,
  reduce,
  tap,
  switchMap,
} from 'rxjs/operators';
import { GoogleTokenService } from './google-token-service';

@Injectable()
export class DataAcquisitionGmailService {
  private logger = new Logger(DataAcquisitionGmailService.name);
  private oAuthClients: OAuth2Client[] = [];
  private gmailClients: gmail_v1.Gmail[] = [];
  private prevTime: number | undefined;

  constructor(private readonly googleTokenService: GoogleTokenService) {
    // Load the credentials
    this.logger.log(`Setting Google base credentials`);

    // Authorize a client with credentials, then call the Gmail API.
    this._authorize();
  }

  @Cron('*/15 * * * * *')
  getLatestEmails(): void {
    this.gmailClients.forEach((client) => this.getLatestEmail(client));
  }

  private async getLatestEmail(client: gmail_v1.Gmail) {
    const result: GaxiosResponse<gmail_v1.Schema$Message>[] = [];
    const list = await client.users.messages.list({
      userId: 'me',
      q: `newer_than:1h`,
    });
    for (let msg of list.data.messages) {
      const fullMsg = await client.users.messages.get({
        userId: 'me',
        id: msg.id,
      });
      result.push(fullMsg);
    }
    result.forEach((msg) =>
      this.logger.log(
        Buffer.from(msg.data.payload.parts[0].body.data, 'base64').toString(
          'binary'
        )
      )
    );
    this.logger.log(result);
    return result;
  }

  getLabels(): Observable<string[]> {
    this._getEmailGists(this.gmailClients[0]).subscribe();
    return forkJoin(
      this.gmailClients.map((client) =>
        client.users.labels.list({ userId: 'me' })
      )
    ).pipe(
      map((results) =>
        results.map((result) => result.data.labels?.map((x) => x.name))
      ),
      map((results) => [].concat(...results))
    );
  }

  getEmails(): Observable<string[]> {
    return forkJoin(
      this.gmailClients.map((client) => this._getEmailGists(client))
    ).pipe(map((results) => [].concat(...results)));
  }

  private _getEmailGists(client: gmail_v1.Gmail) {
    return from(client.users.messages.list({ userId: 'me' }))
      .pipe(
        switchMap((result) =>
          result.data.messages?.map((x) =>
            from(
              client.users.messages.get({
                id: x.id,
                userId: 'me',
              })
            )
          )
        )
      )
      .pipe(tap((val) => console.log(val)));
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
