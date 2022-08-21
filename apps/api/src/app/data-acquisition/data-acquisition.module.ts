import { Module } from '@nestjs/common';
import { DataAcquisitionGmailService } from './data-acquisition-gmail.service';
import { DataAcquisitionService } from './data-acquisition.service';

@Module({
  providers: [DataAcquisitionService, DataAcquisitionGmailService],
})
export class DataAcquisitionModule {}
