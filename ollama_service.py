import os
import json
import logging
import requests
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class OllamaService:
    def __init__(self):
        self.host = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
        self.model = os.getenv('OLLAMA_MODEL', 'qwen2.5:7b')
        self.timeout = int(os.getenv('OLLAMA_TIMEOUT', '30'))
        self.enabled = os.getenv('OLLAMA_ENABLED', 'false').lower() == 'true'
        
        if not self.enabled:
            logging.info("Ollama service disabled")
            return
            
        try:
            self._check_connection()
            logging.info(f"Ollama service initialized: {self.host} with model {self.model}")
        except Exception as e:
            logging.error(f"Failed to initialize Ollama service: {e}")
            self.enabled = False
    
    def _check_connection(self):
        try:
            response = requests.get(f"{self.host}/api/tags", timeout=5)
            if response.status_code != 200:
                raise Exception(f"Ollama API returned {response.status_code}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Cannot connect to Ollama at {self.host}: {e}")
    
    def _make_request(self, prompt: str, data: Optional[Dict[str, Any]] = None) -> str:
        if not self.enabled:
            raise Exception("Ollama service is disabled")
        
        try:
            # Формируем полный промпт с данными
            full_prompt = self._build_prompt(prompt, data)
            
            payload = {
                "model": self.model,
                "prompt": full_prompt,
                "stream": False
            }
            
            response = requests.post(
                f"{self.host}/api/generate",
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"Ollama API error: {response.status_code}")
            
            result = response.json()
            return result.get('response', '').strip()
            
        except requests.exceptions.Timeout:
            raise Exception(f"Ollama request timeout after {self.timeout}s")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Ollama request failed: {e}")
        except Exception as e:
            raise Exception(f"Ollama processing error: {e}")
    
    def _build_prompt(self, prompt: str, data: Optional[Dict[str, Any]] = None) -> str:
        if data:
            data_str = json.dumps(data, ensure_ascii=False, indent=2)
            return f"""{prompt}

Данные для анализа:
{data_str}

Отвечай кратко и по существу на русском языке."""
        return prompt
    
    def analyze_contract(self, contract_data: Dict[str, Any]) -> Dict[str, str]:
        prompt = """Проанализируй контракт и выдай краткую выжимку:
1. Основная суть контракта
2. Ключевые условия (сумма, сроки)
3. Возможные риски или особенности
4. Общая оценка важности"""
        
        try:
            response = self._make_request(prompt, contract_data)
            return {
                'analysis': response,
                'status': 'success'
            }
        except Exception as e:
            logging.error(f"Contract analysis failed: {e}")
            return {
                'analysis': f'Ошибка анализа: {str(e)}',
                'status': 'error'
            }
    
    def validate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        prompt = """Проверь корректность данных контракта:
1. Суммы и цены (реалистичны ли они)
2. Даты (корректны ли форматы и логика)
3. Наименования организаций (есть ли подозрительные моменты)
4. Другие аномалии

Верни результат в виде JSON:
{
  "valid": true/false,
  "issues": ["список проблем"],
  "suggestions": ["список рекомендаций"]
}"""
        
        try:
            response = self._make_request(prompt, data)
            # Пытаемся распарсить JSON ответ
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                return {
                    'valid': False,
                    'issues': [f'Не удалось обработать AI ответ: {response}'],
                    'suggestions': []
                }
        except Exception as e:
            logging.error(f"Data validation failed: {e}")
            return {
                'valid': False,
                'issues': [f'Ошибка валидации: {str(e)}'],
                'suggestions': []
            }
    
    def classify_error(self, error_message: str, context: Optional[Dict[str, Any]] = None) -> str:
        prompt = """Классифицируй ошибку парсинга контракта в одну из категорий:
- NETWORK_ERROR: Проблемы с сетью или доступом
- FORMAT_ERROR: Некорректный формат данных
- MISSING_DATA: Отсутствуют обязательные поля
- VALIDATION_ERROR: Данные не проходят валидацию
- SYSTEM_ERROR: Внутренняя ошибка системы
- UNKNOWN: Неизвестная ошибка

Опиши кратко причину и возможное решение."""
        
        try:
            full_context = {'error': error_message}
            if context:
                full_context.update(context)
            
            response = self._make_request(prompt, full_context)
            return response
        except Exception as e:
            logging.error(f"Error classification failed: {e}")
            return f"UNKNOWN: Ошибка классификации - {str(e)}"
    
    def explain_error(self, error_message: str, contract_data: Optional[Dict[str, Any]] = None) -> str:
        prompt = """Объясни ошибку простыми словами для пользователя.
Что пошло не так и как это можно исправить?
Будь кратким и конструктивным."""
        
        try:
            context = {'error': error_message}
            if contract_data:
                context['contract'] = contract_data
            
            response = self._make_request(prompt, context)
            return response
        except Exception as e:
            logging.error(f"Error explanation failed: {e}")
            return "Не удалось получить объяснение ошибки. Попробуйте повторить запрос позже."

# Глобальный экземпляр сервиса
ollama_service = OllamaService()