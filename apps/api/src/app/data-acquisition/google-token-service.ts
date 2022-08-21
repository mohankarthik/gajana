import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import {
  GoogleToken,
  GoogleTokenDocument,
} from './schemas/google-token-schema';

@Injectable()
export class GoogleTokenService {
  constructor(
    @InjectModel(GoogleToken.name)
    private gooleTokenModel: Model<GoogleTokenDocument>
  ) {}

  async findAll(): Promise<GoogleToken[]> {
    return await this.gooleTokenModel.find().exec();
  }
}
