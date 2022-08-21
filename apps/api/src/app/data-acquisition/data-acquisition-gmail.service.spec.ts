import { Test, TestingModule } from '@nestjs/testing';
import { DataAcquisitionGmailService } from './data-acquisition-gmail.service';

describe('DataAcquisitionGmailService', () => {
  let service: DataAcquisitionGmailService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [DataAcquisitionGmailService],
    }).compile();

    service = module.get<DataAcquisitionGmailService>(
      DataAcquisitionGmailService
    );
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
