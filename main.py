


if __name__ == '__main__':
    for file in ['sal_sp.jsonoutput-1-to-1.json']:
        print(json)
        for address, page in get_pages_from_file(open_results_file(f"./{file}")):
            tokens = get_tokens_from_words(get_words_from_results(page))
            tokens = Tokens([token for token in tokens if token.top < 0.6])
            for n, table in enumerate(tokens.get_tables()):
                print(n)
                # try:
                table.save_csv(f"{file}_{address.replace('/', '_')}_{n}")
                # except Exception as e:
                #     rai
        