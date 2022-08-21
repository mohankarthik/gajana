import { Injectable } from '@nestjs/common';
import { DataAcquisitionGmailService } from './data-acquisition/data-acquisition-gmail.service';
@Injectable()
export class AppService {
  constructor(
    private readonly dataAcquisitionGmailSerivce: DataAcquisitionGmailService
  ) {}
  async getData(): Promise<string[]> {
    return this.dataAcquisitionGmailSerivce.getLabels();
  }
}
