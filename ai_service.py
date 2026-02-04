import os
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from ollama_service import ollama_service

load_dotenv()

class AIService:
    def __init__(self):
        self.primary_service = os.getenv('PRIMARY_AI_SERVICE', 'ollama')
        self.fallback_enabled = os.getenv('OLLAMA_FALLBACK_ONLY', 'false').lower() == 'true'
        
        # Проверяем доступность основных AI сервисов
        self.openai_available = self._check_openai()
        self.claude_available = False  # TODO: добавить если нужно
        self.gemini_available = False  # TODO: добавить если нужно
        
        logging.info(f"AI Service initialized. Primary: {self.primary_service}, Fallback only: {self.fallback_enabled}")
    
    def _check_openai(self) -> bool:
        """Проверяем доступность OpenAI API ключа"""
        try:
            import openai
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key and api_key != 'your_key_here':
                client = openai.OpenAI(api_key=api_key)
                # Простая проверка доступа
                client.models.list()
                logging.info("OpenAI service available")
                return True
        except Exception as e:
            logging.warning(f"OpenAI service unavailable: {e}")
        return False
    
    def _process_with_openai(self, prompt: str, data: Optional[Dict[str, Any]] = None) -> str:
        """Обработка через OpenAI API"""
        try:
            import openai
            client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            
            # Формируем полный промпт
            if data:
                import json
                data_str = json.dumps(data, ensure_ascii=False, indent=2)
                full_prompt = f"{prompt}\n\nДанные для анализа:\n{data_str}"
            else:
                full_prompt = prompt
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Ты - помощник для анализа российских государственных контрактов. Отвечай кратко и по существу на русском языке."},
                    {"role": "user", "content": full_prompt}
                ],
                max_tokens=500,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logging.error(f"OpenAI processing failed: {e}")
            raise
    
    def _choose_service(self):
        """Выбираем основной сервис"""
        if self.fallback_enabled:
            return 'ollama'
        
        if self.primary_service == 'openai' and self.openai_available:
            return 'openai'
        elif self.primary_service == 'claude' and self.claude_available:
            return 'claude'
        elif self.primary_service == 'gemini' and self.gemini_available:
            return 'gemini'
        else:
            # Fallback к Ollama если основной недоступен
            if ollama_service.enabled:
                logging.info(f"Primary service {self.primary_service} unavailable, falling back to Ollama")
                return 'ollama'
            else:
                raise Exception("No AI services available")
    
    def process_with_fallback(self, prompt: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Основной метод обработки с fallback логикой
        """
        last_error = None
        
        # Пробуем основной сервис
        try:
            service = self._choose_service()
            logging.info(f"Attempting AI processing with {service}")
            
            if service == 'openai':
                result = self._process_with_openai(prompt, data)
                return {
                    'result': result,
                    'service': 'openai',
                    'status': 'success'
                }
            elif service == 'ollama':
                result = ollama_service._make_request(prompt, data)
                return {
                    'result': result,
                    'service': 'ollama',
                    'status': 'success'
                }
                
        except Exception as e:
            last_error = e
            logging.warning(f"Primary AI service failed: {e}")
            
            # Пробуем Ollama как fallback если это был не Ollama
            if service != 'ollama' and ollama_service.enabled:
                try:
                    logging.info("Attempting fallback to Ollama")
                    result = ollama_service._make_request(prompt, data)
                    return {
                        'result': result,
                        'service': 'ollama',
                        'status': 'fallback_success'
                    }
                except Exception as fallback_error:
                    last_error = fallback_error
                    logging.error(f"Ollama fallback also failed: {fallback_error}")
        
        # Все сервисы недоступны
        error_msg = f"All AI services failed. Last error: {str(last_error)}"
        logging.error(error_msg)
        return {
            'result': None,
            'service': 'none',
            'status': 'failed',
            'error': error_msg
        }
    
    def analyze_contract(self, contract_data: Dict[str, Any]) -> Dict[str, Any]:
        prompt = """Проанализируй контракт и выдай краткую выжимку:
1. Основная суть контракта
2. Ключевые условия (сумма, сроки)
3. Возможные риски или особенности
4. Общая оценка важности
        
Отвечай кратко и по существу на русском языке."""
        
        return self.process_with_fallback(prompt, contract_data)
    
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
        
        result = self.process_with_fallback(prompt, data)
        
        if result['status'] == 'success':
            # Пытаемся распарсить JSON ответ
            try:
                import json
                parsed_result = json.loads(result['result'])
                return {
                    **parsed_result,
                    'service': result['service'],
                    'status': result['status']
                }
            except json.JSONDecodeError:
                return {
                    'valid': False,
                    'issues': [f'Не удалось обработать AI ответ: {result["result"]}'],
                    'suggestions': [],
                    'service': result['service'],
                    'status': 'parse_error'
                }
        
        return {
            'valid': False,
            'issues': [f'Ошибка валидации: {result.get("error", "Unknown error")}'],
            'suggestions': [],
            'service': result.get('service', 'none'),
            'status': result.get('status', 'failed')
        }
    
    def classify_error(self, error_message: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        prompt = """Классифицируй ошибку парсинга контракта в одну из категорий:
- NETWORK_ERROR: Проблемы с сетью или доступом
- FORMAT_ERROR: Некорректный формат данных
- MISSING_DATA: Отсутствуют обязательные поля
- VALIDATION_ERROR: Данные не проходят валидацию
- SYSTEM_ERROR: Внутренняя ошибка системы
- UNKNOWN: Неизвестная ошибка

Верни результат в виде JSON:
{
  "category": "категория ошибки",
  "explanation": "краткое объяснение",
  "suggestion": "рекомендация по исправлению"
}"""
        
        full_context = {'error': error_message}
        if context:
            full_context.update(context)
        
        result = self.process_with_fallback(prompt, full_context)
        
        if result['status'] == 'success':
            # Пытаемся распарсить JSON ответ
            try:
                import json
                parsed_result = json.loads(result['result'])
                return {
                    **parsed_result,
                    'service': result['service']
                }
            except json.JSONDecodeError:
                return {
                    'category': 'UNKNOWN',
                    'explanation': f'Не удалось обработать AI ответ: {result["result"]}',
                    'suggestion': 'Проверьте данные и повторите запрос',
                    'service': result['service']
                }
        
        return {
            'category': 'UNKNOWN',
            'explanation': f'Ошибка классификации: {result.get("error", "Unknown error")}',
            'suggestion': 'Попробуйте повторить запрос позже',
            'service': result.get('service', 'none')
        }
    
    def explain_error(self, error_message: str, contract_data: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        prompt = """Объясни ошибку простыми словами для пользователя.
Что пошло не так и как это можно исправить?
Будь кратким и конструктивным."""
        
        context = {'error': error_message}
        if contract_data:
            context['contract'] = contract_data
        
        result = self.process_with_fallback(prompt, context)
        
        return {
            'explanation': result['result'] if result['status'] == 'success' else 'Не удалось получить объяснение ошибки. Попробуйте повторить запрос позже.',
            'service': result['service'],
            'status': result['status']
        }

# Глобальный экземпляр AI сервиса
ai_service = AIService()