import { S3 } from 'aws-sdk';
import { promisify } from 'util';

const _s3: { [region: string]: S3Async } = {};

export class S3Async {
  private _s3: S3;
  private headBucketAsync: (params: S3.Types.HeadBucketRequest) => Promise<any>;
  private createBucketAsync: (params: S3.Types.CreateBucketRequest) => Promise<S3.Types.CreateBucketOutput>;
  private objectExistsAsync: (params: S3.Types.HeadObjectRequest) => Promise<S3.Types.HeadObjectOutput>;
  private putObjectAsync: (params: S3.Types.PutObjectRequest) => Promise<S3.Types.PutObjectOutput>;
  private getObjectAsync: (params: S3.Types.GetObjectRequest) => Promise<S3.Types.GetObjectOutput>;
  private deleteObjectAsync: (params: S3.Types.DeleteObjectRequest) => Promise<S3.Types.DeleteObjectOutput>;
  private listObjectsAsync: (params: S3.Types.ListObjectsV2Request) => Promise<S3.Types.ListObjectsV2Output>;

  private constructor(s3: S3) {
    this._s3 = s3;
    this.headBucketAsync = promisify(this._s3.headBucket).bind(this._s3);
    this.createBucketAsync = promisify(this._s3.createBucket).bind(this._s3);
    this.objectExistsAsync = promisify(this._s3.headObject).bind(this._s3);
    this.putObjectAsync = promisify(this._s3.putObject).bind(this._s3);
    this.getObjectAsync = promisify(this._s3.getObject).bind(this._s3);
    this.deleteObjectAsync = promisify(this._s3.deleteObject).bind(this._s3);
    this.listObjectsAsync = promisify(this._s3.listObjectsV2).bind(this._s3);
  }

  static getInstance(endpoint: string, region: string): S3Async {
    const config = {
      s3ForcePathStyle: true,
      s3DisableBodySigning: true,
      region: region,
      endpoint: endpoint,
      apiVersion: '2006-03-01'
    };
    _s3[config.region] = _s3[config.region] || new S3Async(new S3(config));
    return _s3[config.region];
  }

  async listObjects(bucketName: string, prefix: string, maxKeys?: number, continuationToken?: string) {
    return await this.listObjectsAsync({
      Bucket: bucketName,
      Prefix: prefix,
      Delimiter: '/',
      MaxKeys: maxKeys,
      ContinuationToken: continuationToken
    });
  }

  async listObjectsRecursive(bucketName: string, prefix: string, maxKeys?: number, continuationToken?: string) {
    return await this.listObjectsAsync({
      Bucket: bucketName,
      Prefix: prefix,
      MaxKeys: maxKeys,
      ContinuationToken: continuationToken
    });
  }

  async bucketExists(bucketName: string): Promise<boolean> {
    try {
      await this.headBucketAsync({ Bucket: bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async createBucket(bucketName: string): Promise<boolean> {
    try {
      await this.createBucketAsync({ Bucket: bucketName });
    } catch (e) {
      return false;
    }
    return true;
  }

  async objectExists(bucketName: string, key: string): Promise<boolean> {
    try {
      const response = await this.objectExistsAsync({ Bucket: bucketName, Key: key });
      return (response.ContentLength || 0) > 0;
    } catch (e) {
      return false;
    }
  }

  async putObject(bucketName: string, key: string, data: any): Promise<S3.PutObjectOutput> {
    return await this.putObjectAsync({
      Key: key,
      Bucket: bucketName,
      Body: data
    });
  }

  async upload(bucketName: string, key: string, data: any, acl: string = 'public-read') {
    return new Promise<S3.ManagedUpload.SendData>((resolve, reject) => {
      const managedUpload = this._s3.upload({
        Key: key,
        Bucket: bucketName,
        Body: data,
        ACL: acl
      });
      managedUpload.send((error, data) => {
        if (!!error) reject(error);
        resolve(data);
      });
    });
  }

  async getObject(bucketName: string, key: string): Promise<S3.GetObjectOutput> {
    return await this.getObjectAsync({ Bucket: bucketName, Key: key });
  }

  async deleteObject(bucketName: string, key: string): Promise<S3.DeleteObjectOutput> {
    return await this.deleteObjectAsync({ Bucket: bucketName, Key: key });
  }
}
