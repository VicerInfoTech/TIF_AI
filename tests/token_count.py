import tiktoken
import transformers
from transformers import AutoTokenizer
import os

def count_tokens(text, method='tiktoken', model_name='cl100k_base'):
    """
    Count tokens in text using different tokenization methods
    
    Args:
        text (str): The text to count tokens for
        method (str): Tokenization method - 'tiktoken', 'huggingface', or 'word'
        model_name (str): Model name for tokenizer (for tiktoken or huggingface)
        
    Returns:
        dict: Token count information
    """
    
    if method == 'tiktoken':
        try:
            # For OpenAI models
            encoding = tiktoken.get_encoding(model_name)
            tokens = encoding.encode(text)
            return {
                'method': 'tiktoken',
                'model': model_name,
                'token_count': len(tokens),
                'tokens': tokens if len(tokens) <= 100 else tokens[:100]  # Sample of first 100 tokens
            }
        except Exception as e:
            return {'error': f"Tiktoken error: {str(e)}"}
    
    elif method == 'huggingface':
        try:
            # For Hugging Face models
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            tokens = tokenizer.encode(text)
            return {
                'method': 'huggingface',
                'model': model_name,
                'token_count': len(tokens),
                'tokens': tokens if len(tokens) <= 100 else tokens[:100]  # Sample of first 100 tokens
            }
        except Exception as e:
            return {'error': f"HuggingFace error: {str(e)}"}
    
    elif method == 'word':
        # Simple word-based tokenization
        words = text.split()
        characters = len(text)
        return {
            'method': 'word',
            'word_count': len(words),
            'character_count': characters,
            'average_word_length': characters / len(words) if words else 0
        }
    
    else:
        return {'error': 'Invalid method. Use "tiktoken", "huggingface", or "word"'}

def analyze_text_file_tokens(file_path, methods=['tiktoken', 'word'], models=None):
    """
    Analyze token count for a text file using multiple methods
    
    Args:
        file_path (str): Path to the text file
        methods (list): List of methods to use
        models (dict): Dictionary mapping methods to model names
        
    Returns:
        dict: Comprehensive token analysis
    """
    
    if models is None:
        models = {
            'tiktoken': 'cl100k_base',  # Default for GPT-4
            'huggingface': 'bert-base-uncased'
        }
    
    # Check if file exists
    if not os.path.exists(file_path):
        return {'error': f"File '{file_path}' not found."}
    
    try:
        # Read the text file
        with open(file_path, 'r', encoding='utf-8') as file:
            text = file.read()
        
        # Basic text statistics
        text_stats = {
            'file_path': file_path,
            'file_size_bytes': os.path.getsize(file_path),
            'character_count': len(text),
            'line_count': len(text.splitlines()),
            'word_count': len(text.split()),
            'methods': {}
        }
        
        # Count tokens using each method
        for method in methods:
            model_name = models.get(method)
            if method == 'word':
                result = count_tokens(text, method='word')
            else:
                result = count_tokens(text, method=method, model_name=model_name)
            
            text_stats['methods'][method] = result
        
        return text_stats
        
    except Exception as e:
        return {'error': f"Error analyzing file: {str(e)}"}

def compare_token_methods(file_path):
    """
    Compare different tokenization methods for a file
    
    Args:
        file_path (str): Path to the text file
        
    Returns:
        dict: Comparison results
    """
    
    # Common models for comparison
    models_config = {
        'tiktoken': [
            'cl100k_base',  # GPT-4, GPT-3.5-turbo
            'p50k_base',    # Code models
            'r50k_base'     # GPT-3
        ],
        'huggingface': [
            'bert-base-uncased',
            'gpt2',
            'sentence-transformers/all-mpnet-base-v2'
        ]
    }
    
    results = {}
    
    # Read the file
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            text = file.read()
    except Exception as e:
        return {'error': f"Error reading file: {str(e)}"}
    
    # Compare tiktoken models
    results['tiktoken'] = {}
    for model in models_config['tiktoken']:
        try:
            encoding = tiktoken.get_encoding(model)
            tokens = encoding.encode(text)
            results['tiktoken'][model] = {
                'token_count': len(tokens),
                'tokens_per_character': len(tokens) / len(text) if text else 0
            }
        except Exception as e:
            results['tiktoken'][model] = {'error': str(e)}
    
    # Compare huggingface models
    results['huggingface'] = {}
    for model in models_config['huggingface']:
        try:
            tokenizer = AutoTokenizer.from_pretrained(model)
            tokens = tokenizer.encode(text)
            results['huggingface'][model] = {
                'token_count': len(tokens),
                'tokens_per_character': len(tokens) / len(text) if text else 0
            }
        except Exception as e:
            results['huggingface'][model] = {'error': str(e)}
    
    # Word count for reference
    results['word'] = {
        'word_count': len(text.split()),
        'character_count': len(text)
    }
    
    return results

# Example usage and utility functions
def print_token_analysis(analysis):
    """Pretty print token analysis results"""
    
    if 'error' in analysis:
        print(f"Error: {analysis['error']}")
        return
    
    print("=" * 60)
    print("TOKEN ANALYSIS REPORT")
    print("=" * 60)
    print(f"File: {analysis['file_path']}")
    print(f"File Size: {analysis['file_size_bytes']:,} bytes")
    print(f"Characters: {analysis['character_count']:,}")
    print(f"Words: {analysis['word_count']:,}")
    print(f"Lines: {analysis['line_count']:,}")
    print()
    
    for method, result in analysis['methods'].items():
        print(f"{method.upper()} METHOD:")
        if 'error' in result:
            print(f"  Error: {result['error']}")
        elif method == 'word':
            print(f"  Word Count: {result['word_count']:,}")
            print(f"  Character Count: {result['character_count']:,}")
            print(f"  Average Word Length: {result['average_word_length']:.2f}")
        else:
            print(f"  Model: {result['model']}")
            print(f"  Token Count: {result['token_count']:,}")
            if 'tokens' in result:
                print(f"  First 10 tokens: {result['tokens'][:10]}")
        print()

# Main function to check tokens
def check_file_tokens(file_path):
    """
    Main function to check token count of a file
    
    Args:
        file_path (str): Path to the text file
        
    Returns:
        dict: Token analysis results
    """
    
    print(f"Analyzing tokens for: {file_path}")
    print()
    
    # Basic analysis with default methods
    analysis = analyze_text_file_tokens(
        file_path, 
        methods=['tiktoken', 'word'],
        models={'tiktoken': 'cl100k_base'}
    )
    
    print_token_analysis(analysis)
    
    # Optional: Detailed comparison
    print("Would you like to see detailed comparison across models? (y/n)")
    if input().lower().startswith('y'):
        comparison = compare_token_methods(file_path)
        print("\nDETAILED COMPARISON:")
        print("=" * 50)
        
        for method, models in comparison.items():
            if method == 'word':
                print(f"\nWORD COUNT:")
                print(f"  Words: {models['word_count']:,}")
                print(f"  Characters: {models['character_count']:,}")
            else:
                print(f"\n{method.upper()} MODELS:")
                for model_name, stats in models.items():
                    if 'error' in stats:
                        print(f"  {model_name}: Error - {stats['error']}")
                    else:
                        print(f"  {model_name}: {stats['token_count']:,} tokens "
                              f"({stats['tokens_per_character']:.3f} tokens/char)")
    
    return analysis

# Install required packages if needed
def install_required_packages():
    """Helper function to install required packages"""
    
    packages = [
        'tiktoken',
        'transformers',
        'torch'
    ]
    
    for package in packages:
        try:
            __import__(package)
            print(f"✓ {package} is already installed")
        except ImportError:
            print(f"Installing {package}...")
            import subprocess
            import sys
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])

if __name__ == "__main__":
    # Install packages if needed
    install_required_packages()
    
    # Example usage
    file_path_1 = r"BoxMaster_minimal.txt"  # Your generated text file
    file_path_2 = r"BoxMaster_structured.txt"  # Your generated text file
     # Check if file exists
    if not os.path.exists(file_path_1) and not os.path.exists(file_path_2):
        print(f"File '{file_path_1}' and '{file_path_2}' not found. Please generate them first using the YAML converter.")
    else:
        # Run token analysis
        results_1 = check_file_tokens(file_path_1)
        results_2 = check_file_tokens(file_path_2)