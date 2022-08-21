import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';
import { Credentials } from 'google-auth-library';

export type GoogleTokenDocument = GoogleToken & Document;

@Schema()
export class GoogleToken implements Credentials {
  @Prop({ required: true })
  access_token: string;

  @Prop({ required: true })
  refresh_token: string;

  @Prop({ required: true })
  expiry_date: number;

  @Prop({ required: true })
  token_type: string;

  @Prop({ required: true })
  scope: string;
}

export const GoogleTokenSchema = SchemaFactory.createForClass(GoogleToken);
