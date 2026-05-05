import { useState, useEffect, useRef } from 'react'
import ReactMarkdown from 'react-markdown'

function App() {
  const [messages, setMessages] = useState([
    { role: 'assistant', content: "Hello! I'm your financial assistant. Ask me anything about Apple's 10-K." }
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const scrollRef = useRef(null);

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendMessage = async () => {
    console.log("VITE_QUERY_URL:", import.meta.env.VITE_QUERY_URL); // Is this actually a URL?
    if (!input.trim()) return;

    const userMessage = { role: 'user', content: input };
    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      // Replace with your actual Lambda Function URL or API Gateway endpoint
      const baseUrl = import.meta.env.VITE_QUERY_URL;
      const response = await fetch(`${baseUrl}?q=${encodeURIComponent(input)}`);

      // const response = await fetch(`YOUR_LAMBDA_URL?q=${encodeURIComponent(input)}`);
      const data = await response.json();
      
      setMessages(prev => [...prev, { role: 'assistant', content: data.answer }]);
    } catch (error) {
      setMessages(prev => [...prev, { role: 'assistant', content: "Error: Could not reach the assistant." }]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-screen max-w-3xl mx-auto p-4 bg-gray-50">
      <header className="py-4 border-b">
        <h1 className="text-xl font-bold text-gray-800">Financial Assistant MVP</h1>
      </header>

      {/* Chat History Field */}
      <div className="flex-1 overflow-y-auto my-4 p-4 bg-white rounded-lg shadow-inner space-y-4">
        {messages.map((msg, idx) => (
          <div key={idx} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            <div className={`max-w-[80%] p-3 rounded-lg ${msg.role === 'user' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-800'}`}>
              <ReactMarkdown>
                {msg.content}
              </ReactMarkdown>
            </div>
          </div>
        ))}
        {isLoading && <div className="text-gray-400 italic text-sm">Assistant is thinking...</div>}
        <div ref={scrollRef} />
      </div>

      {/* Input Field */}
      <div className="flex gap-2 p-2 bg-white border rounded-lg shadow-sm">
        <input
          className="flex-1 outline-none px-2 py-1 text-gray-700"
          type="text"
          value={input}
          placeholder="Ask a question..."
          onKeyDown={(e) => e.key === 'Enter' && sendMessage()}
          onChange={(e) => setInput(e.target.value)}
          disabled={isLoading}
        />
        <button 
          onClick={sendMessage}
          disabled={isLoading}
          className="bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 disabled:bg-gray-400 transition-colors"
        >
          Send
        </button>
      </div>
    </div>
  )
}

export default App
