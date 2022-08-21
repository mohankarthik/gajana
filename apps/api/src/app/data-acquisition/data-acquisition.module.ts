import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { DataAcquisitionGmailService } from './data-acquisition-gmail.service';
import { DataAcquisitionService } from './data-acquisition.service';
import { GoogleTokenService } from './google-token-service';
import { GoogleToken, GoogleTokenSchema } from './schemas/google-token-schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: GoogleToken.name, schema: GoogleTokenSchema },
    ]),
  ],
  providers: [
    DataAcquisitionService,
    DataAcquisitionGmailService,
    GoogleTokenService,
  ],
  exports: [DataAcquisitionGmailService],
})
export class DataAcquisitionModule {}
