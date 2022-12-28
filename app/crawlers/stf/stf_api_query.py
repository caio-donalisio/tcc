def get_query(start_date, end_date, term='a OR (de OR o)', offset=0, size=10):
  return {
    "query": {
      "function_score": {
        "functions": [{
          "exp": {
            "julgamento_data": {
              "origin": "now",
              "scale": "47450d",
              "offset": "1095d",
              "decay": 0.1
            }
          }
        }, {
          "filter": {
            "term": {
              "orgao_julgador.keyword": "Tribunal Pleno"
            }
          },
          "weight": 1.15
        }, {
          "filter": {
            "term": {
              "is_repercussao_geral": True
            }
          },
          "weight": 1.1
        }],
        "query": {
          "bool": {
            "filter": [{
              "query_string": {
                "default_operator": "AND",
                "fields": ["acordao_ata.plural^3", "documental_acordao_mesmo_sentido_lista_texto.plural", "documental_doutrina_texto.plural", "documental_indexacao_texto.plural", "documental_jurisprudencia_citada_texto.plural", "documental_legislacao_citada_texto.plural", "documental_observacao_texto.plural", "documental_publicacao_lista_texto.plural", "documental_tese_tema_texto.plural^3", "documental_tese_texto.plural^3", "ementa_texto.plural^3", "ministro_facet.plural", "revisor_processo_nome.plural", "orgao_julgador.plural", "partes_lista_texto.plural", "procedencia_geografica_completo.plural", "processo_classe_processual_unificada_extenso.plural", "titulo.plural^6", "colac_numero.plural", "colac_pagina.plural", "decisao_texto.plural^2", "documental_decisao_mesmo_sentido_lista_texto.plural", "processo_precedente_texto.plural", "sumula_texto.plural^3", "conteudo_texto.plural"],
                "query": term,
                "type": "cross_fields",
                "fuzziness": "AUTO:4,7",
                "analyzer": "legal_search_analyzer",
                "quote_analyzer": "legal_index_analyzer"
              }
            }],
            "must": [{
                "range": {
                  "publicacao_data": {
                    "gte": start_date,
                    "lte": end_date
                  }
                }
              }
            ],
            "should": [{
              "query_string": {
                "default_operator": "AND",
                "fields": ["acordao_ata.plural^3", "documental_doutrina_texto.plural", "documental_indexacao_texto.plural", "documental_jurisprudencia_citada_texto.plural", "documental_observacao_texto.plural", "documental_tese_tema_texto.plural^3", "documental_tese_texto.plural^3", "ementa_texto.plural^3", "titulo.plural^6", "decisao_texto.plural^2", "sumula_texto.plural^3", "conteudo_texto.plural"],
                "query": term,
                "tie_breaker": 1,
                "fuzziness": "AUTO:4,7",
                "analyzer": "legal_search_analyzer",
                "quote_analyzer": "legal_index_analyzer"
              }
            }, {
              "query_string": {
                "default_operator": "and",
                "type": "phrase",
                "tie_breaker": 1,
                "phrase_slop": 20,
                "fields": ["acordao_ata.plural^3", "documental_tese_tema_texto.plural^3", "documental_tese_texto.plural^3", "ementa_texto.plural^3", "decisao_texto.plural^2", "conteudo_texto.plural"],
                "query": term,
                "fuzziness": "AUTO:4,7",
                "analyzer": "legal_search_analyzer",
                "quote_analyzer": "legal_index_analyzer"
              }
            }, {
              "query_string": {
                "default_operator": "and",
                "type": "phrase",
                "tie_breaker": 1,
                "phrase_slop": 5,
                "fields": ["documental_acordao_mesmo_sentido_lista_texto.plural", "documental_doutrina_texto.plural", "documental_indexacao_texto.plural", "documental_jurisprudencia_citada_texto.plural", "documental_legislacao_citada_texto.plural", "documental_observacao_texto.plural", "partes_lista_texto.plural", "processo_precedente_texto.plural", "documental_decisao_mesmo_sentido_lista_texto.plural"],
                "query": term,
                "fuzziness": "AUTO:4,7",
                "analyzer": "legal_search_analyzer",
                "quote_analyzer": "legal_index_analyzer"
              }
            }, {
              "query_string": {
                "default_operator": "and",
                "type": "phrase",
                "phrase_slop": 1,
                "fields": ["documental_publicacao_lista_texto.plural", "ministro_facet.plural", "revisor_processo_nome.plural", "orgao_julgador.plural", "procedencia_geografica_completo.plural", "processo_classe_processual_unificada_extenso.plural", "titulo.plural^6", "colac_numero.plural", "colac_pagina.plural", "sumula_texto.plural^3"],
                "query": term,
                "fuzziness": "AUTO:4,7",
                "boost": 0,
                "analyzer": "legal_search_analyzer",
                "quote_analyzer": "legal_index_analyzer"
              }
            }]
          }
        }
      }
    },
    "_source": ["base", "_id", "id", "dg_unique", "titulo", "ministro_facet", "procedencia_geografica_completo", "procedencia_geografica_pais_sigla", "procedencia_geografica_uf_sigla", "procedencia_geografica_uf_extenso", "processo_codigo_completo", "processo_classe_processual_unificada_extenso", "processo_classe_processual_unificada_classe_sigla", "processo_classe_processual_unificada_incidente_sigla", "processo_numero", "julgamento_data", "publicacao_data", "is_decisao_presidencia", "relator_processo_nome", "presidente_nome", "relator_decisao_nome", "acordao_ata", "decisao_texto", "partes_lista_texto", "acompanhamento_processual_url", "dje_url", "documental_publicacao_lista_texto", "documental_decisao_mesmo_sentido_lista_texto", "documental_decisao_mesmo_sentido_is_secundario", "documental_legislacao_citada_texto", "documental_indexacao_texto", "documental_observacao_texto", "documental_doutrina_texto", "externo_seq_objeto_incidente", "dg_atualizado_em", "informativo_nome", "informativo_numero", "informativo_url", "periodo_inicio_data", "periodo_fim_data", "conteudo_texto", "conteudo_html", "processo_lista_texto", "sumula_numero", "orgao_julgador", "is_vinculante", "sumula_texto", "processo_precedente_texto", "processo_precedente_html", "processo_classe_processual_unificada_sigla", "is_questao_ordem", "is_repercussao_geral_admissibilidade", "is_repercussao_geral_merito", "is_repercussao_geral", "is_processo_antigo", "is_colac", "colac_numero", "colac_pagina", "revisor_processo_nome", "relator_acordao_nome", "julgamento_is_sessao_virtual", "republicacao_data", "ementa_texto", "inteiro_teor_url", "documental_acordao_mesmo_sentido_lista_texto", "documental_acordao_mesmo_sentido_is_secundario", "documental_jurisprudencia_citada_texto", "documental_assunto_texto", "documental_tese_tipo", "documental_tese_texto", "documental_tese_tema_texto", "old_seq_colac", "old_seq_repercussao_geral", "old_seq_sjur"],
    "aggs": {
      "base_agg": {
        "filters": {
          "filters": {
            "acordaos": {
              "match": {
                "base": "acordaos"
              }
            },
            "sumulas": {
              "match": {
                "base": "sumulas"
              }
            },
            "decisoes": {
              "match": {
                "base": "decisoes"
              }
            },
            "informativos": {
              "match": {
                "base": "informativos"
              }
            }
          }
        }
      },
      "is_repercussao_geral_agg": {
        "filters": {
          "filters": {
            "true": {
              "match": {
                "is_repercussao_geral": True
              }
            },
            "false": {
              "match": {
                "is_repercussao_geral": False
              }
            }
          }
        }
      },
      "is_repercussao_geral_admissibilidade_agg": {
        "filters": {
          "filters": {
            "true": {
              "match": {
                "is_repercussao_geral_admissibilidade": True
              }
            },
            "false": {
              "match": {
                "is_repercussao_geral_admissibilidade": False
              }
            }
          }
        }
      },
      "is_repercussao_geral_merito_agg": {
        "filters": {
          "filters": {
            "true": {
              "match": {
                "is_repercussao_geral_merito": True
              }
            },
            "false": {
              "match": {
                "is_repercussao_geral_merito": False
              }
            }
          }
        }
      },
      "is_questao_ordem_agg": {
        "filters": {
          "filters": {
            "true": {
              "match": {
                "is_questao_ordem": True
              }
            },
            "false": {
              "match": {
                "is_questao_ordem": False
              }
            }
          }
        }
      },
      "is_colac_agg": {
        "filters": {
          "filters": {
            "true": {
              "match": {
                "is_colac": True
              }
            },
            "false": {
              "match": {
                "is_colac": False
              }
            }
          }
        }
      },
      "orgao_julgador_agg": {
        "aggs": {
          "orgao_julgador_agg": {
            "terms": {
              "field": "orgao_julgador.keyword",
              "size": 200,
              "execution_hint": "map"
            }
          }
        },
        "filter": {
          "bool": {
            "must": [{
              "term": {
                "base": "acordaos"
              }
            }]
          }
        }
      },
      "ministro_facet_agg": {
        "aggs": {
          "ministro_facet_agg": {
            "terms": {
              "field": "ministro_facet.keyword",
              "size": 200,
              "execution_hint": "map"
            }
          }
        },
        "filter": {
          "bool": {
            "must": [{
              "term": {
                "base": "acordaos"
              }
            }]
          }
        }
      },
      "processo_classe_processual_unificada_classe_sigla_agg": {
        "aggs": {
          "processo_classe_processual_unificada_classe_sigla_agg": {
            "terms": {
              "field": "processo_classe_processual_unificada_classe_sigla.keyword",
              "size": 200,
              "execution_hint": "map"
            }
          }
        },
        "filter": {
          "bool": {
            "must": [{
              "term": {
                "base": "acordaos"
              }
            }]
          }
        }
      },
      "procedencia_geografica_uf_sigla_agg": {
        "aggs": {
          "procedencia_geografica_uf_sigla_agg": {
            "terms": {
              "field": "procedencia_geografica_uf_sigla",
              "size": 200,
              "execution_hint": "map"
            }
          }
        },
        "filter": {
          "bool": {
            "must": [{
              "term": {
                "base": "acordaos"
              }
            }]
          }
        }
      }
    },
    "size": size,
    "from": offset,
    "post_filter": {
      "bool": {
        "must": [{
          "term": {
            "base": "acordaos"
          }
        }],
        "should": []
      }
    },
    "sort": [{
      "publicacao_data": "desc"
    }],
    "highlight": {
      "highlight_query": {
        "bool": {
          "filter": [{
            "query_string": {
              "default_operator": "AND",
              "fields": ["acordao_ata.plural^3", "documental_acordao_mesmo_sentido_lista_texto.plural", "documental_doutrina_texto.plural", "documental_indexacao_texto.plural", "documental_jurisprudencia_citada_texto.plural", "documental_legislacao_citada_texto.plural", "documental_observacao_texto.plural", "documental_publicacao_lista_texto.plural", "documental_tese_tema_texto.plural^3", "documental_tese_texto.plural^3", "ementa_texto.plural^3", "ministro_facet.plural", "revisor_processo_nome.plural", "orgao_julgador.plural", "partes_lista_texto.plural", "procedencia_geografica_completo.plural", "processo_classe_processual_unificada_extenso.plural", "titulo.plural^6", "colac_numero.plural", "colac_pagina.plural", "decisao_texto.plural^2", "documental_decisao_mesmo_sentido_lista_texto.plural", "processo_precedente_texto.plural", "sumula_texto.plural^3", "conteudo_texto.plural"],
              "query": "de",
              "type": "cross_fields",
              "fuzziness": "AUTO:4,7",
              "analyzer": "legal_search_analyzer",
              "quote_analyzer": "legal_index_analyzer"
            }
          }],
          "must": [],
          "should": [{
            "query_string": {
              "default_operator": "AND",
              "fields": ["acordao_ata.plural^3", "documental_doutrina_texto.plural", "documental_indexacao_texto.plural", "documental_jurisprudencia_citada_texto.plural", "documental_observacao_texto.plural", "documental_tese_tema_texto.plural^3", "documental_tese_texto.plural^3", "ementa_texto.plural^3", "titulo.plural^6", "decisao_texto.plural^2", "sumula_texto.plural^3", "conteudo_texto.plural"],
              "query": "de",
              "tie_breaker": 1,
              "fuzziness": "AUTO:4,7",
              "analyzer": "legal_search_analyzer",
              "quote_analyzer": "legal_index_analyzer"
            }
          }, {
            "query_string": {
              "default_operator": "and",
              "type": "phrase",
              "tie_breaker": 1,
              "phrase_slop": 20,
              "fields": ["acordao_ata.plural^3", "documental_tese_tema_texto.plural^3", "documental_tese_texto.plural^3", "ementa_texto.plural^3", "decisao_texto.plural^2", "conteudo_texto.plural"],
              "query": "de",
              "fuzziness": "AUTO:4,7",
              "analyzer": "legal_search_analyzer",
              "quote_analyzer": "legal_index_analyzer"
            }
          }, {
            "query_string": {
              "default_operator": "and",
              "type": "phrase",
              "tie_breaker": 1,
              "phrase_slop": 5,
              "fields": ["documental_acordao_mesmo_sentido_lista_texto.plural", "documental_doutrina_texto.plural", "documental_indexacao_texto.plural", "documental_jurisprudencia_citada_texto.plural", "documental_legislacao_citada_texto.plural", "documental_observacao_texto.plural", "partes_lista_texto.plural", "processo_precedente_texto.plural", "documental_decisao_mesmo_sentido_lista_texto.plural"],
              "query": "de",
              "fuzziness": "AUTO:4,7",
              "analyzer": "legal_search_analyzer",
              "quote_analyzer": "legal_index_analyzer"
            }
          }, {
            "query_string": {
              "default_operator": "and",
              "type": "phrase",
              "phrase_slop": 1,
              "fields": ["documental_publicacao_lista_texto.plural", "ministro_facet.plural", "revisor_processo_nome.plural", "orgao_julgador.plural", "procedencia_geografica_completo.plural", "processo_classe_processual_unificada_extenso.plural", "titulo.plural^6", "colac_numero.plural", "colac_pagina.plural", "sumula_texto.plural^3"],
              "query": "de",
              "fuzziness": "AUTO:4,7",
              "boost": 0,
              "analyzer": "legal_search_analyzer",
              "quote_analyzer": "legal_index_analyzer"
            }
          }]
        }
      },
      "number_of_fragments": 64,
      "fragment_size": 300,
      "order": "score",
      "pre_tags": ["<em>"],
      "post_tags": ["</em>"],
      "fields": {
        "ementa_texto": {
          "fragment_size": 2400,
          "matched_fields": ["ementa_texto.plural"],
          "type": "fvh"
        },
        "sumula_texto": {
          "number_of_fragments": 0,
          "matched_fields": ["sumula_texto.plural"],
          "type": "fvh"
        },
        "conteudo_texto": {
          "fragment_size": 1200,
          "matched_fields": ["conteudo_texto.plural"],
          "type": "fvh"
        },
        "acordao_ata": {
          "fragment_size": 600,
          "matched_fields": ["acordao_ata.plural"],
          "type": "fvh"
        },
        "decisao_texto": {
          "fragment_size": 1200,
          "matched_fields": ["decisao_texto.plural"],
          "type": "fvh"
        },
        "documental_tese_texto": {
          "fragment_size": 2000,
          "matched_fields": ["documental_tese_texto.plural"],
          "type": "fvh"
        },
        "documental_tese_tema_texto": {
          "fragment_size": 2000,
          "matched_fields": ["documental_tese_tema_texto.plural"],
          "type": "fvh"
        },
        "documental_observacao_texto": {
          "matched_fields": ["documental_observacao_texto.plural"],
          "type": "fvh"
        },
        "documental_indexacao_texto": {
          "matched_fields": ["documental_indexacao_texto.plural"],
          "type": "fvh"
        },
        "documental_legislacao_citada_texto": {
          "matched_fields": ["documental_legislacao_citada_texto.plural"],
          "type": "fvh"
        },
        "documental_jurisprudencia_citada_texto": {
          "matched_fields": ["documental_jurisprudencia_citada_texto.plural"],
          "type": "fvh"
        },
        "documental_doutrina_texto": {
          "matched_fields": ["documental_doutrina_texto.plural"],
          "type": "fvh"
        },
        "partes_lista_texto": {
          "matched_fields": ["partes_lista_texto.plural"],
          "type": "fvh"
        },
        "documental_publicacao_lista_texto": {
          "matched_fields": ["documental_publicacao_lista_texto.plural"],
          "type": "fvh"
        },
        "documental_acordao_mesmo_sentido_lista_texto": {
          "matched_fields": ["documental_acordao_mesmo_sentido_lista_texto.plural"],
          "type": "fvh"
        },
        "documental_decisao_mesmo_sentido_lista_texto": {
          "matched_fields": ["documental_decisao_mesmo_sentido_lista_texto.plural"],
          "type": "fvh"
        },
        "processo_precedente_texto": {
          "matched_fields": ["processo_precedente_texto.plural"],
          "type": "fvh"
        },
        "procedencia_geografica_completo": {
          "matched_fields": ["procedencia_geografica_completo.plural"],
          "type": "fvh"
        }
      }
    },
    "track_total_hits": True
  }