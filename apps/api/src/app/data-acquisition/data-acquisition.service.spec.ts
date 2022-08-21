import { Test, TestingModule } from '@nestjs/testing';
import { DataAcquisitionService } from './data-acquisition.service';

describe('DataAcquisitionService', () => {
  let service: DataAcquisitionService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [DataAcquisitionService],
    }).compile();

    service = module.get<DataAcquisitionService>(DataAcquisitionService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
