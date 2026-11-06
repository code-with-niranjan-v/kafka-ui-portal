import { useEffect, useMemo, useState } from 'react';

function App() {
  const [health, setHealth] = useState(null);
  const [topics, setTopics] = useState([]);
  const [status, setStatus] = useState('Ready');
  const [loading, setLoading] = useState({ health: false, topics: false, create: false, produce: false, consume: false });
  const [topicName, setTopicName] = useState('demo-events');
  const [partitions, setPartitions] = useState(3);
  const [replicationFactor, setReplicationFactor] = useState(1);
  const [messageTopic, setMessageTopic] = useState('demo-events');
  const [messageText, setMessageText] = useState('Hello from Kafka UI!');
  const [consumeTopic, setConsumeTopic] = useState('demo-events');
  const [maxMessages, setMaxMessages] = useState(10);
  const [messages, setMessages] = useState([]);

  const metrics = useMemo(() => {
    return {
      brokerState: health?.brokerStatus || 'unknown',
      kafkaHome: health?.kafkaHome || 'Not resolved',
      bootstrapServer: health?.bootstrapServer || 'localhost:9092',
      topicsCount: topics.length,
    };
  }, [health, topics]);

  const apiRequest = async (url, options = {}) => {
    const response = await fetch(url, {
      headers: { 'Content-Type': 'application/json' },
      ...options,
    });

    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(data.message || 'Request failed');
    }

    return data;
  };

  const refreshHealth = async () => {
    setLoading((prev) => ({ ...prev, health: true }));
    try {
      const result = await apiRequest('/api/health');
      setHealth(result);
      if (Array.isArray(result.topics)) {
        setTopics(result.topics);
      }
      setStatus(result.message || 'Cluster health refreshed');
    } catch (error) {
      setStatus(error.message || 'Unable to connect to the Kafka API');
    } finally {
      setLoading((prev) => ({ ...prev, health: false }));
    }
  };

  const refreshTopics = async () => {
    setLoading((prev) => ({ ...prev, topics: true }));
    try {
      const result = await apiRequest('/api/topics');
      setTopics(result.topics || []);
      setStatus('Topics refreshed');
    } catch (error) {
      setStatus(error.message || 'Unable to load topics');
    } finally {
      setLoading((prev) => ({ ...prev, topics: false }));
    }
  };

  const startBroker = async () => {
    try {
      setStatus('Starting Kafka broker...');
      const result = await apiRequest('/api/broker/start', { method: 'POST' });
      setStatus(result.message || 'Broker started');
      await refreshHealth();
    } catch (error) {
      setStatus(error.message); 
    }
  };

  const stopBroker = async () => {
    try {
      setStatus('Stopping Kafka broker...');
      const result = await apiRequest('/api/broker/stop', { method: 'POST' });
      setStatus(result.message || 'Broker stopped');
      await refreshHealth();
    } catch (error) {
      setStatus(error.message); 
    }
  };

  const createTopic = async (event) => {
    event.preventDefault();
    setLoading((prev) => ({ ...prev, create: true }));
    try {
      const result = await apiRequest('/api/topics/create', {
        method: 'POST',
        body: JSON.stringify({
          topic: topicName,
          partitions: Number(partitions),
          replicationFactor: Number(replicationFactor),
        }),
      });
      setStatus(result.output || `Topic ${topicName} created successfully`);
      await refreshTopics();
      setMessageTopic(topicName);
      setConsumeTopic(topicName);
    } catch (error) {
      setStatus(error.message || 'Unable to create topic');
    } finally {
      setLoading((prev) => ({ ...prev, create: false }));
    }
  };

  const produceMessage = async (event) => {
    event.preventDefault();
    setLoading((prev) => ({ ...prev, produce: true }));
    try {
      const result = await apiRequest('/api/messages/produce', {
        method: 'POST',
        body: JSON.stringify({
          topic: messageTopic,
          message: messageText,
        }),
      });
      setStatus(result.message || 'Message sent');
    } catch (error) {
      setStatus(error.message || 'Unable to publish message');
    } finally {
      setLoading((prev) => ({ ...prev, produce: false }));
    }
  };

  const consumeMessages = async (event) => {
    event.preventDefault();
    setLoading((prev) => ({ ...prev, consume: true }));
    try {
      const params = new URLSearchParams({
        topic: consumeTopic,
        maxMessages: String(maxMessages),
      });
      const result = await apiRequest(`/api/messages/consume?${params.toString()}`);
      setMessages(result.messages || []);
      setStatus(`Fetched ${result.messages?.length || 0} messages from ${consumeTopic}`);
    } catch (error) {
      setStatus(error.message || 'Unable to consume messages');
      setMessages([]);
    } finally {
      setLoading((prev) => ({ ...prev, consume: false }));
    }
  };

  useEffect(() => {
    refreshHealth();
  }, []);

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Kafka operations</p>
          <h1>Kafka UI Portal</h1>
        </div>
        <div className="topbar-actions">
          <button type="button" className="secondary" onClick={refreshHealth} disabled={loading.health}>
            {loading.health ? 'Refreshing...' : 'Refresh health'}
          </button>
          <button type="button" onClick={startBroker}>Start broker</button>
          <button type="button" className="danger" onClick={stopBroker}>Stop broker</button>
        </div>
      </header>

      <section className="status-row">
        <div className="status-card">
          <span className="label">Broker status</span>
          <strong className={metrics.brokerState === 'running' ? 'success-text' : 'warning-text'}>{metrics.brokerState}</strong>
        </div>
        <div className="status-card">
          <span className="label">Bootstrap server</span>
          <strong>{metrics.bootstrapServer}</strong>
        </div>
        <div className="status-card">
          <span className="label">Kafka home</span>
          <strong>{metrics.kafkaHome}</strong>
        </div>
        <div className="status-card">
          <span className="label">Topics</span>
          <strong>{metrics.topicsCount}</strong>
        </div>
      </section>

      <div className="status-banner">{status}</div>

      <main className="panel-grid">
        <section className="panel">
          <div className="panel-header">
            <h2>Create topic</h2>
            <button type="button" className="link-button" onClick={refreshTopics}>Refresh</button>
          </div>

          <form onSubmit={createTopic} className="stacked-form">
            <label>
              Topic name
              <input value={topicName} onChange={(event) => setTopicName(event.target.value)} />
            </label>
            <div className="two-column">
              <label>
                Partitions
                <input type="number" min="1" value={partitions} onChange={(event) => setPartitions(event.target.value)} />
              </label>
              <label>
                Replication factor
                <input type="number" min="1" value={replicationFactor} onChange={(event) => setReplicationFactor(event.target.value)} />
              </label>
            </div>
            <button type="submit" disabled={loading.create}>{loading.create ? 'Creating...' : 'Create topic'}</button>
          </form>
        </section>

        <section className="panel">
          <div className="panel-header">
            <h2>Topic list</h2>
          </div>
          <div className="topic-list">
            {topics.length === 0 ? (
              <p className="muted">No topics available yet.</p>
            ) : (
              topics.map((topic) => (
                <div key={topic} className="topic-pill">{topic}</div>
              ))
            )}
          </div>
        </section>

        <section className="panel wide-panel">
          <div className="panel-header">
            <h2>Produce message</h2>
          </div>
          <form onSubmit={produceMessage} className="stacked-form">
            <div className="two-column">
              <label>
                Topic
                <input value={messageTopic} onChange={(event) => setMessageTopic(event.target.value)} />
              </label>
            </div>
            <label>
              Message payload
              <textarea rows="5" value={messageText} onChange={(event) => setMessageText(event.target.value)} />
            </label>
            <button type="submit" disabled={loading.produce}>{loading.produce ? 'Publishing...' : 'Send message'}</button>
          </form>
        </section>

        <section className="panel wide-panel">
          <div className="panel-header">
            <h2>Consume messages</h2>
          </div>
          <form onSubmit={consumeMessages} className="stacked-form">
            <div className="two-column">
              <label>
                Topic
                <input value={consumeTopic} onChange={(event) => setConsumeTopic(event.target.value)} />
              </label>
              <label>
                Max messages
                <input type="number" min="1" value={maxMessages} onChange={(event) => setMaxMessages(event.target.value)} />
              </label>
            </div>
            <button type="submit" disabled={loading.consume}>{loading.consume ? 'Loading...' : 'View messages'}</button>
          </form>

          <div className="message-box">
            {messages.length === 0 ? (
              <p className="muted">No messages fetched yet.</p>
            ) : (
              messages.map((message, index) => (
                <div key={`${message}-${index}`} className="message-row">
                  <span className="message-index">#{index + 1}</span>
                  <span>{message}</span>
                </div>
              ))
            )}
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
