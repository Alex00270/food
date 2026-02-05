#!/usr/bin/env python3
import os
import requests

BASE_URL = os.getenv('LLM_BASE_URL', 'http://172.86.90.213:3000/v1')
API_KEY = os.getenv('LLM_API_KEY', '')
MODEL_PRIMARY = os.getenv('LLM_MODEL_PRIMARY', 'openai/gpt-oss-120b')
MODEL_FALLBACK = os.getenv('LLM_MODEL_FALLBACK', 'llama-3.3-70b-versatile')
TIMEOUT = int(os.getenv('LLM_TIMEOUT', '60'))
OLLAMA_URL = os.getenv('OLLAMA_URL', 'http://localhost:11434/api/generate')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'tinyllama')


def _post_chat(payload):
    headers = {
        'Content-Type': 'application/json'
    }
    if API_KEY:
        headers['Authorization'] = f"Bearer {API_KEY}"
    url = f"{BASE_URL}/chat/completions"
    resp = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"LLM error {resp.status_code}: {resp.text}")
    return resp.json()

def _post_ollama(prompt):
    payload = {
        'model': OLLAMA_MODEL,
        'prompt': prompt,
        'stream': False
    }
    resp = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"Ollama error {resp.status_code}: {resp.text}")
    data = resp.json()
    return data.get('response', '').strip()


def chat(messages, model=None):
    model = model or MODEL_PRIMARY
    payload = {
        'model': model,
        'messages': messages,
        'temperature': 0.2,
        'max_tokens': 800
    }
    try:
        data = _post_chat(payload)
        return data
    except Exception:
        if model != MODEL_FALLBACK:
            payload['model'] = MODEL_FALLBACK
            data = _post_chat(payload)
            return data
        # Fallback to local Ollama if gateway fails
        prompt = "\n".join([f"{m['role']}: {m['content']}" for m in messages])
        response = _post_ollama(prompt)
        return {
            'choices': [{
                'message': {'content': response}
            }]
        }


def classify_contract_type(text):
    messages = [
        {
            'role': 'system',
            'content': 'Ты классификатор типов контрактов. Ответь одним словом из списка: питание, строительство, поставка, услуги, прочее.'
        },
        {
            'role': 'user',
            'content': text[:4000]
        }
    ]
    data = chat(messages)
    return data['choices'][0]['message']['content'].strip().lower()


if __name__ == '__main__':
    sample = 'Обучающиеся 1-4 классов – завтрак и обед. Услуги питания.'
    print(classify_contract_type(sample))
