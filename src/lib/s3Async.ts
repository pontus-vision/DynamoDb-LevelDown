import { S3 } from 'aws-sdk';
import { promisify } from 'util';
import { Attachment } from './types';

const NOOP_PROMISE = <T>() => new Promise<T>(() => undefined);

export class S3Async {
  private headBucketAsync: (params: S3.Types.HeadBucketRequest) => Promise<any>;
  private createBucketAsync: (params: S3.Types.CreateBucketRequest) => Promise<S3.Types.CreateBucketOutput>;
  private objectExistsAsync: (params: S3.Types.HeadObjectRequest) => Promise<S3.Types.HeadObjectOutput>;
  private putObjectAsync: (params: S3.Types.PutObjectRequest) => Promise<S3.Types.PutObjectOutput>;
  private getObjectAsync: (params: S3.Types.GetObjectRequest) => Promise<S3.Types.GetObjectOutput>;
  private deleteObjectAsync: (params: S3.Types.DeleteObjectRequest) => Promise<S3.Types.DeleteObjectOutput>;
  private listObjectsAsync: (params: S3.Types.ListObjectsV2Request) => Promise<S3.Types.ListObjectsV2Output>;
  private deleteBucketAsync: (params: S3.Types.DeleteBucketRequest) => Promise<any>;

  constructor(private s3: S3, private bucketName: string) {
    if (!!s3) {
      this.putObjectAsync = promisify(this.s3.putObject).bind(this.s3);
      this.getObjectAsync = promisify(this.s3.getObject).bind(this.s3);
      this.headBucketAsync = promisify(this.s3.headBucket).bind(this.s3);
      this.objectExistsAsync = promisify(this.s3.headObject).bind(this.s3);
      this.createBucketAsync = promisify(this.s3.createBucket).bind(this.s3);
      this.deleteObjectAsync = promisify(this.s3.deleteObject).bind(this.s3);
      this.listObjectsAsync = promisify(this.s3.listObjectsV2).bind(this.s3);
      this.deleteBucketAsync = promisify(this.s3.deleteBucket).bind(this.s3);
    } else {
      this.putObjectAsync = NOOP_PROMISE;
      this.getObjectAsync = NOOP_PROMISE;
      this.headBucketAsync = NOOP_PROMISE;
      this.objectExistsAsync = NOOP_PROMISE;
      this.createBucketAsync = NOOP_PROMISE;
      this.deleteObjectAsync = NOOP_PROMISE;
      this.listObjectsAsync = NOOP_PROMISE;
      this.deleteBucketAsync = NOOP_PROMISE;
    }
  }

  static get noop() {
    return new S3Async((undefined as unknown) as S3, (undefined as unknown) as string);
  }

  private get isNoop() {
    return !this.s3;
  }

  async listObjects(prefix: string, maxKeys?: number, continuationToken?: string) {
    if (this.isNoop === true) return;
    return await this.listObjectsAsync({
      Bucket: this.bucketName,
      Prefix: prefix,
      Delimiter: '/',
      MaxKeys: maxKeys,
      ContinuationToken: continuationToken
    });
  }

  async listObjectsRecursive(prefix: string, maxKeys?: number, continuationToken?: string) {
    if (this.isNoop === true) return;
    return await this.listObjectsAsync({
      Bucket: this.bucketName,
      Prefix: prefix,
      MaxKeys: maxKeys,
      ContinuationToken: continuationToken
    });
  }

  async bucketExists() {
    if (this.isNoop === true) return true;
    try {
      await this.headBucketAsync({ Bucket: this.bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async createBucket() {
    if (this.isNoop === true) return true;
    try {
      await this.createBucketAsync({ Bucket: this.bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async deleteBucket() {
    if (this.isNoop === true) return true;
    try {
      await this.deleteBucketAsync({ Bucket: this.bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async objectExists(key: string) {
    if (this.isNoop === true) return true;
    try {
      const response = await this.objectExistsAsync({ Bucket: this.bucketName, Key: key });
      return !!response.ContentLength;
    } catch (e) {
      return false;
    }
  }

  async upload(key: string, data: any, acl: S3.ObjectCannedACL = 'public-read') {
    if (this.isNoop === true) return;
    return new Promise<S3.ManagedUpload.SendData>((resolve, reject) => {
      const managedUpload = this.s3.upload({
        Key: key,
        Bucket: this.bucketName,
        Body: data,
        ACL: acl
      });
      managedUpload.send((error, data) => {
        if (!!error) reject(error);
        resolve(data);
      });
    });
  }

  async putObject(key: string, data: any, contentType?: string, acl: S3.ObjectCannedACL = 'public-read') {
    if (this.isNoop === true) return;
    const putResult = await this.putObjectAsync({
      Key: key,
      Bucket: this.bucketName,
      Body: data,
      ContentType: contentType,
      ACL: acl
    });
    return putResult;
  }

  async putObjectBatch(...attachments: Attachment[]) {
    if (this.isNoop === true) return;
    return await Promise.all(
      attachments.map(a => this.putObject(a.key, a.data, a.contentType).then(result => ({ key: a.key, result })))
    );
  }

  async getObject(key: string) {
    if (this.isNoop === true) return;
    return await this.getObjectAsync({ Bucket: this.bucketName, Key: key });
  }

  async getObjectBatch(...keys: string[]): Promise<{ [key: string]: S3.GetObjectOutput }> {
    if (this.isNoop === true) return {};
    return await Promise.all(keys.map(key => this.getObject(key).then(result => ({ key, result })))).then(all =>
      all.reduce((p, c) => ({ ...p, [c.key]: c.result }), {})
    );
  }

  async deleteObject(key: string) {
    if (this.isNoop === true) return;
    return await this.deleteObjectAsync({ Bucket: this.bucketName, Key: key });
  }

  async deleteObjectBatch(...keys: string[]): Promise<{ [key: string]: S3.DeleteObjectOutput }> {
    if (this.isNoop === true) return {};
    return await Promise.all(keys.map(key => this.deleteObject(key).then(result => ({ key, result })))).then(all =>
      all.reduce((p, c) => ({ ...p, [c.key]: c.result }), {})
    );
  }
}
