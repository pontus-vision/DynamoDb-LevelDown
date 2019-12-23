import { S3 } from 'aws-sdk';
import { promisify } from 'util';
import { Attachment } from './types';

/* @internal */
export class S3Async {
  private headBucketAsync?: (params: S3.Types.HeadBucketRequest) => Promise<any>;
  private createBucketAsync?: (params: S3.Types.CreateBucketRequest) => Promise<S3.Types.CreateBucketOutput>;
  private putObjectAsync?: (params: S3.Types.PutObjectRequest) => Promise<S3.Types.PutObjectOutput>;
  private getObjectAsync?: (params: S3.Types.GetObjectRequest) => Promise<S3.Types.GetObjectOutput>;
  private deleteObjectAsync?: (params: S3.Types.DeleteObjectRequest) => Promise<S3.Types.DeleteObjectOutput>;
  private deleteBucketAsync?: (params: S3.Types.DeleteBucketRequest) => Promise<any>;

  constructor(private s3: S3, private bucketName: string) {
    if (!!s3) {
      this.putObjectAsync = promisify(this.s3.putObject).bind(this.s3);
      this.getObjectAsync = promisify(this.s3.getObject).bind(this.s3);
      this.headBucketAsync = promisify(this.s3.headBucket).bind(this.s3);
      this.createBucketAsync = promisify(this.s3.createBucket).bind(this.s3);
      this.deleteObjectAsync = promisify(this.s3.deleteObject).bind(this.s3);
      this.deleteBucketAsync = promisify(this.s3.deleteBucket).bind(this.s3);
    }
  }

  async bucketExists() {
    if (!this.headBucketAsync) return true;
    try {
      await this.headBucketAsync({ Bucket: this.bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async createBucket() {
    if (!this.createBucketAsync) return true;
    try {
      await this.createBucketAsync({ Bucket: this.bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async deleteBucket() {
    if (!this.deleteBucketAsync) return true;
    try {
      await this.deleteBucketAsync({ Bucket: this.bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async putObject(key: string, data: any, contentType?: string, acl: S3.ObjectCannedACL = 'public-read') {
    if (!this.putObjectAsync) return {};
    const putResult = await this.putObjectAsync({
      Key: key,
      Bucket: this.bucketName,
      Body: data,
      ContentType: contentType,
      ACL: acl
    });
    return putResult;
  }

  async putObjectBatch(...attachments: Attachment[]): Promise<{ [key: string]: S3.PutObjectOutput }> {
    if (!this.putObjectAsync) return {};
    return await Promise.all(
      attachments.map(a => this.putObject(a.key, a.data, a.contentType).then(result => ({ key: a.key, result })))
    ).then(all => all.reduce((p, c) => ({ ...p, [c.key]: c.result }), {}));
  }

  async getObject(key: string) {
    if (!this.getObjectAsync) return {};
    return await this.getObjectAsync({ Bucket: this.bucketName, Key: key });
  }

  async getObjectBatch(...keys: string[]): Promise<{ [key: string]: S3.GetObjectOutput }> {
    if (!this.getObjectAsync) return {};
    return await Promise.all(keys.map(key => this.getObject(key).then(result => ({ key, result })))).then(all =>
      all.reduce((p, c) => ({ ...p, [c.key]: c.result }), {})
    );
  }

  async deleteObject(key: string) {
    if (!this.deleteObjectAsync) return {};
    return await this.deleteObjectAsync({ Bucket: this.bucketName, Key: key });
  }

  async deleteObjectBatch(...keys: string[]): Promise<{ [key: string]: S3.DeleteObjectOutput }> {
    if (!this.deleteObjectAsync) return {};
    return await Promise.all(keys.map(key => this.deleteObject(key).then(result => ({ key, result })))).then(all =>
      all.reduce((p, c) => ({ ...p, [c.key]: c.result }), {})
    );
  }
}
