import { Injectable } from '@nestjs/common';
import { Observable } from 'rxjs';
import { DataAcquisitionGmailService } from './data-acquisition/data-acquisition-gmail.service';
@Injectable()
export class AppService {
  constructor(
    private readonly dataAcquisitionGmailSerivce: DataAcquisitionGmailService
  ) {}
  getData(): Observable<string[]> {
    return this.dataAcquisitionGmailSerivce.getLabels();
  }
}
