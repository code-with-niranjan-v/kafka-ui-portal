import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import net from 'net';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

function resolveKafkaRoot() {
  const candidates = [
    process.env.KAFKA_HOME,
    path.resolve(projectRoot, '..', 'kafka_2.13-4.4.0'),
    path.resolve(projectRoot, '..', 'kafka-4.4.0'),
    path.resolve(projectRoot, '..', 'kafka_2.13-4.4.0', 'bin'),
    path.resolve(projectRoot, '..', 'kafka-4.4.0', 'bin')
  ].filter(Boolean);

  for (const candidate of candidates) {
    const binDir = path.join(candidate, 'bin');
    const hasBinaryScripts = fs.existsSync(binDir) && fs.existsSync(path.join(binDir, 'kafka-server-start.sh')) || fs.existsSync(path.join(binDir, 'kafka-server-start.bat'));
    if (fs.existsSync(candidate) && (hasBinaryScripts || fs.existsSync(path.join(candidate, 'libs')))) {
      return candidate;
    }
  }

  return path.resolve(projectRoot, '..', 'kafka_2.13-4.4.0');
}

const kafkaRoot = resolveKafkaRoot();
const isWindows = process.platform === 'win32';
const defaultBootstrapServer = 'localhost:9092';

const app = express();
const port = process.env.PORT || 3001;

app.use(cors());
app.use(express.json({ limit: '2mb' }));

function getKafkaScript(scriptName) {
  const candidateDirs = [
    path.join(kafkaRoot, 'bin'),
    path.join(kafkaRoot, 'bin', 'windows')
  ];

  const preferred = isWindows
    ? ['.sh', '.bat']
    : ['.sh'];

  for (const dir of candidateDirs) {
    for (const suffix of preferred) {
      const candidate = path.join(dir, `${scriptName}${suffix}`);
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    }
  }

  return null;
}

function getGitBashPath() {
  const candidates = [
    'C:/Program Files/Git/bin/bash.exe',
    'C:/Program Files/Git/usr/bin/bash.exe',
    'C:/msys64/usr/bin/bash.exe'
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

function toUnixPath(value) {
  return String(value || '').replace(/\\/g, '/');
}

function toGitBashPath(value) {
  return toUnixPath(value).replace(/^([A-Za-z]):\//, (_, drive) => `/${drive.toLowerCase()}/`);
}

function getKafkaLog4jOptions() {
  const log4jPath = toUnixPath(path.join(kafkaRoot, 'config', 'log4j2.yaml'));
  return `-Dlog4j2.configurationFile=file:${log4jPath.match(/^[A-Za-z]:\//) ? `/${log4jPath}` : log4jPath}`;
}

function getKafkaRuntimeEnv(extra = {}) {
  return {
    ...process.env,
    ...extra,
    KAFKA_HOME: kafkaRoot,
    KAFKA_LOG4J_OPTS: process.env.KAFKA_LOG4J_OPTS || getKafkaLog4jOptions(),
    KAFKA_HEAP_OPTS: process.env.KAFKA_HEAP_OPTS || '-Xmx1G -Xms256M'
  };
}

function quoteForShell(value) {
  if (!value || !/[\s"'&|<>]/.test(value)) {
    return value;
  }

  return `"${value.replace(/"/g, '\\"')}"`;
}

function quoteForBash(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function buildGitBashCommand(scriptPath, args = []) {
  const bashScript = toGitBashPath(scriptPath);
  const bashArgs = args.map(quoteForBash).join(' ');

  return [
    `export KAFKA_HOME='${toGitBashPath(kafkaRoot)}'`,
    `export KAFKA_LOG4J_OPTS='${(process.env.KAFKA_LOG4J_OPTS || getKafkaLog4jOptions()).replace(/'/g, `'\\''`)}'`,
    `export KAFKA_HEAP_OPTS='${(process.env.KAFKA_HEAP_OPTS || '-Xmx1G -Xms256M').replace(/'/g, `'\\''`)}'`,
    `exec "${bashScript}" ${bashArgs}`
  ].join('; ');
}

function getConfiguredBroker() {
  return process.env.KAFKA_BOOTSTRAP_SERVER || defaultBootstrapServer;
}

function isKafkaAvailable() {
  return !!getKafkaScript('kafka-topics') || !!getKafkaScript('kafka-server-start');
}

function isPortOpen(portNumber) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    socket.setTimeout(1200);

    socket.on('connect', () => {
      socket.destroy();
      resolve(true);
    });

    socket.on('timeout', () => {
      socket.destroy();
      resolve(false);
    });

    socket.on('error', () => {
      resolve(false);
    });

    socket.connect(portNumber, '127.0.0.1');
  });
}

function runKafkaCommand(scriptName, args = [], options = {}) {
  const scriptPath = getKafkaScript(scriptName);

  if (!scriptPath) {
    throw new Error(
      `Kafka script ${scriptName} was not found. Set KAFKA_HOME to your Kafka installation or use the bundled distribution. ` +
      `The app currently expects a Kafka installation at ${kafkaRoot}.`
    );
  }

  return new Promise((resolve, reject) => {
    const gitBash = getGitBashPath();
    const useGitBash = isWindows && scriptPath.endsWith('.sh') && !!gitBash;

    const child = useGitBash
      ? spawn(gitBash, ['-lc', buildGitBashCommand(scriptPath, args)], {
          cwd: kafkaRoot,
          env: getKafkaRuntimeEnv(),
          stdio: ['pipe', 'pipe', 'pipe']
        })
      : spawn(scriptPath, args, {
          cwd: kafkaRoot,
          env: getKafkaRuntimeEnv(),
          shell: isWindows,
          stdio: ['pipe', 'pipe', 'pipe']
        });

    let stdout = '';
    let stderr = '';

    if (options.input) {
      child.stdin.write(options.input);
      child.stdin.end();
    }

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', (error) => reject(error));
    child.on('close', (code) => {
      if (code === 0) {
        resolve({ stdout: stdout.trim(), stderr: stderr.trim() });
      } else {
        reject(new Error(stderr.trim() || stdout.trim() || `Process exited with code ${code}`));
      }
    });
  });
}

app.get('/api/health', async (req, res) => {
  try {
    const bootstrapServer = getConfiguredBroker();
    const brokerStatus = await isPortOpen(9092) ? 'running' : 'stopped';
    const topics = await listTopicsSafe();

    res.json({
      kafkaHome: kafkaRoot,
      bootstrapServer,
      brokerStatus,
      kafkaAvailable: isKafkaAvailable(),
      topics,
      message: brokerStatus === 'running' ? 'Kafka appears to be running.' : 'Kafka is not running yet. Start it from the dashboard.'
    });
  } catch (error) {
    res.json({
      kafkaHome: kafkaRoot,
      bootstrapServer: getConfiguredBroker(),
      brokerStatus: 'unknown',
      kafkaAvailable: false,
      topics: [],
      message: error.message
    });
  }
});

async function listTopicsSafe() {
  try {
    const result = await runKafkaCommand('kafka-topics', ['--list', '--bootstrap-server', getConfiguredBroker()]);
    return result.stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
  } catch (error) {
    return [];
  }
}

app.post('/api/broker/start', async (req, res) => {
  try {
    const scriptPath = getKafkaScript('kafka-server-start');
    const serverConfig = path.join(kafkaRoot, 'config', 'server.properties');

    if (!scriptPath) {
      return res.status(400).json({ message: 'Kafka server start script not found. Set KAFKA_HOME to the Kafka installation directory.' });
    }

    if (!fs.existsSync(serverConfig)) {
      return res.status(400).json({ message: `Kafka broker config not found at ${serverConfig}.` });
    }

    const gitBash = getGitBashPath();
    const useGitBash = isWindows && scriptPath.endsWith('.sh') && !!gitBash;

    const child = useGitBash
      ? spawn(gitBash, ['-lc', buildGitBashCommand(scriptPath, [serverConfig])], {
          cwd: kafkaRoot,
          env: getKafkaRuntimeEnv(),
          detached: true,
          stdio: 'ignore'
        })
      : spawn(scriptPath, [serverConfig], {
          cwd: kafkaRoot,
          env: getKafkaRuntimeEnv(),
          detached: true,
          shell: isWindows,
          stdio: 'ignore'
        });

    child.unref();

    res.json({
      success: true,
      message: 'Kafka broker startup command has been issued.'
    });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

app.post('/api/broker/stop', async (req, res) => {
  try {
    const stopScript = getKafkaScript('kafka-server-stop');

    if (!stopScript) {
      return res.status(400).json({ message: 'Kafka stop script not available in this installation.' });
    }

    await runKafkaCommand('kafka-server-stop', []);
    res.json({ success: true, message: 'Kafka broker shutdown request sent.' });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

app.post('/api/topics/create', async (req, res) => {
  try {
    const { topic, partitions = 1, replicationFactor = 1 } = req.body || {};

    if (!topic || !topic.trim()) {
      return res.status(400).json({ message: 'A topic name is required.' });
    }

    const args = [
      '--create',
      '--bootstrap-server',
      getConfiguredBroker(),
      '--topic',
      topic,
      '--partitions',
      String(partitions),
      '--replication-factor',
      String(replicationFactor)
    ];

    const result = await runKafkaCommand('kafka-topics', args);
    res.json({ success: true, output: result.stdout || result.stderr || 'Topic created.' });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

app.get('/api/topics', async (req, res) => {
  try {
    const topics = await listTopicsSafe();
    res.json({ topics });
  } catch (error) {
    res.status(500).json({ message: error.message, topics: [] });
  }
});

app.post('/api/messages/produce', async (req, res) => {
  try {
    const { topic, message } = req.body || {};

    if (!topic || !message) {
      return res.status(400).json({ message: 'Topic and message text are required.' });
    }

    const scriptPath = getKafkaScript('kafka-console-producer');
    const gitBash = getGitBashPath();
    const useGitBash = isWindows && scriptPath.endsWith('.sh') && !!gitBash;

    const producer = useGitBash
      ? spawn(gitBash, ['-lc', buildGitBashCommand(scriptPath, [
          '--bootstrap-server',
          getConfiguredBroker(),
          '--topic',
          topic
        ])], {
          cwd: kafkaRoot,
          env: getKafkaRuntimeEnv(),
          stdio: ['pipe', 'pipe', 'pipe']
        })
      : spawn(scriptPath, [
          '--bootstrap-server',
          getConfiguredBroker(),
          '--topic',
          topic
        ], {
          cwd: kafkaRoot,
          env: getKafkaRuntimeEnv(),
          shell: isWindows,
          stdio: ['pipe', 'pipe', 'pipe']
        });

    let stderr = '';
    producer.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    producer.stdin.write(`${message}\n`);
    producer.stdin.end();

    producer.on('error', (error) => {
      throw error;
    });

    producer.on('close', (code) => {
      if (code === 0) {
        res.json({ success: true, message: `Published to ${topic}` });
      } else {
        res.status(500).json({ message: stderr || `Producer exited with code ${code}` });
      }
    });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

app.get('/api/messages/consume', async (req, res) => {
  try {
    const { topic, maxMessages = 10 } = req.query;

    if (!topic) {
      return res.status(400).json({ message: 'A topic name is required.' });
    }

    const result = await runKafkaCommand('kafka-console-consumer', [
      '--bootstrap-server',
      getConfiguredBroker(),
      '--topic',
      topic,
      '--from-beginning',
      '--max-messages',
      String(maxMessages)
    ]);

    const messages = result.stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    res.json({ messages });
  } catch (error) {
    res.status(500).json({ message: error.message, messages: [] });
  }
});

const distPath = path.join(projectRoot, 'dist');
if (fs.existsSync(distPath)) {
  app.use(express.static(distPath));
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api')) {
      return next();
    }
    res.sendFile(path.join(distPath, 'index.html'));
  });
}

app.listen(port, () => {
  console.log(`Kafka UI API is running on http://localhost:${port}`);
  console.log(`Kafka home: ${kafkaRoot}`);
});
