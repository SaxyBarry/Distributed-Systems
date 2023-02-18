import requests
import json

def sendRequest(url):
    # send the GET request with headers specifying that we want JSON back
    response = requests.get(url, headers={'Content-Type': 'application/json'})
    # check the response status code
    if response.status_code == 200:
        # parse the response JSON data
        data = json.loads(response.content)
        # do something with the data
        print(data)
        return data
    else:
        print('Error: Request failed with status code', response.status_code)
        return {'error':'failure'}

def expectedFailures():
    method = 'word-count'
    book = ''
    # When no file provided
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}"
    result = sendRequest(url)
    assert {'error':' was not found'} == result, "Expects Error Return"
    # Invalid file provided
    book = 'thisdoesnotexist'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}"
    result = sendRequest(url)
    assert {'error':'thisdoesnotexist was not found'} == result, "Expects Error Return"
    # No book specified
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}"
    result = sendRequest(url)
    assert {'error':'No book provided'} == result, "Expects Error Return"
    # Book, no method
    book = 'comma-test.txt'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?book={book}"
    result = sendRequest(url)
    assert {'error':'None is an invalid command'} == result, "Expects Error Return"
    # Incorrect Method + Correct Book
    book = 'comma-test.txt'
    method = 'notamethod'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}"
    result = sendRequest(url)
    assert {'error':'notamethod is an invalid command'} == result, "Expects Error Return"
    # URL Incorrect, Correct method, Correct Book
    book = 'comma-test.txt'
    method = 'word-count'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}book={book}"
    result = sendRequest(url)
    assert {'error':'invalid command formatting'} == result, "Expects Error Return"
    # Correct Method + No Directory
    method = 'inverted-index'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}"
    result = sendRequest(url)
    assert {'error':f'No directory provided'} == result, "Expects Error Return"
    # Correct Method + Incorrect Directory
    dir = 'notadir'
    method = 'inverted-index'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&directory={dir}"
    result = sendRequest(url)
    assert {'error':f'directory {dir} was not found'} == result, "Expects Error Return"
  
def expectedPasses():
    # Empty Text File
    method = 'word-count'
    book = 'empty-test.txt'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}"
    result = sendRequest(url)  
    assert result == {'result':{}}, "Expected Empty Return"
    # Empty Text File with specifier
    method = 'word-count'
    book = 'empty-test.txt'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}&specifier=hello"
    result = sendRequest(url)  
    assert result == {'result': {'hello': 0}}, "Expected 0 Hellos"
    # Comma Separated Text File With Hello Specifier
    method = 'word-count'
    book = 'comma-test.txt'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}&specifier=hello"
    result = sendRequest(url)  
    assert result == {'result':{'hello':2}}, "Expected 2 Hellos"
    # 1 Word Text File
    method = 'word-count'
    book = 'single-test.txt'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}&specifier=hello"
    result = sendRequest(url)  
    assert result == {'result':{'hello':1}}, "Expected 1 Hello"
    # 2 Word Text File with Specifier
    method = 'word-count'
    book = 'two-word-test.txt'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}&specifier=hello"
    result = sendRequest(url)  
    assert result == {'result':{'hello':5}}, "Expected 5 Hellos"
    # 2 Word Text File without specifier
    method = 'word-count'
    book = 'two-word-test.txt'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&book={book}"
    result = sendRequest(url)  
    assert result == {"result": {"hello": 5,"world": 5}}, "Expected 5 Hellos, 5 worlds"
    # Empty Dir, no specifier
    method = 'inverted-index'
    dir = 'empty'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&directory={dir}"
    result = sendRequest(url)  
    assert result == {"result": {}}, "Expected no result"
    # Empty Dir + Specifier
    method = 'inverted-index'
    dir = 'empty'
    specifier = 'kafka'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&directory={dir}&specifier={specifier}"
    result = sendRequest(url)  
    assert result == {"result": {'kafka':[]}}, "Expected no results"
    # Full Dir + Specifier
    method = 'inverted-index'
    dir = 'books2'
    specifier = 'fox'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&directory={dir}&specifier={specifier}"
    result = sendRequest(url)  
    assert len(result['result'][specifier]) == 2, "Expected 2 results"
    # Full Dir + Specifier with no instances in files
    method = 'inverted-index'
    dir = 'books2'
    specifier = 'foxdedededed'
    url = f"http://{DATA['ip_address']}:{DATA['server']['server_port']}/?method={method}&directory={dir}&specifier={specifier}"
    result = sendRequest(url)  
    assert len(result['result'][specifier]) == 0, "Expected 0 results"
    
if __name__ == '__main__':
    with open("config.json") as json_data_file:
        DATA = json.load(json_data_file)
    # expectedFailures()
    expectedPasses()
    print("All Tests Pass")