export interface AppConfig {
  readonly port: number;
  readonly databaseUrl: string;
  readonly awsRegion: string;
  readonly sqsEndpoint: string;
}

function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function port(value: string): number {
  const parsed = Number.parseInt(value, 10);

  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }

  return parsed;
}

export function getAppConfig(): AppConfig {
  return {
    port: port(required('PORT', '3000')),
    databaseUrl: required('DATABASE_URL'),
    awsRegion: required('AWS_REGION', 'us-east-1'),
    sqsEndpoint: required('SQS_ENDPOINT'),
  };
}
