import dlt
from dlt.sources.helpers import requests


@dlt.source
def pokemon_source():
    return pokemon_resource()


def _paginated_get(url, headers, max_pages = 100):
    while True:
        response = requests.get(url, headers = headers)
        response.raise_for_status()
        page = response.json()
        print(page)        
        next_page_url = page["next"]
        
        yield page

        max_pages -= 1

        if next_page_url is None or max_pages == 0:
            break
        else:
            url = next_page_url

@dlt.resource(write_disposition="append")
def pokemon_resource():
    # No headers for authorization are needed
    headers = {"User-Agent": "PokemonLookupPython"}

    # URL for getting data about Pikachu from the Pokémon API
    url = "https://pokeapi.co/api/v2/pokemon/"

    # Make the API call
    response = _paginated_get(url, headers=headers)

    for row in response:
        yield row

if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='pokemon', destination='bigquery', dataset_name='pokemon_data'
    )

    # Fetch and print the data by running the resource
    #data = list(pokemon_resource())

    # Print the data yielded from resource
    #print(data)

    # Run the pipeline with your parameters
    load_info = pipeline.run(pokemon_source())

    # Pretty print the information on data that was loaded
    print(load_info)
