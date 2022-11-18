
####################################################
#                                                  #
#   Algoritmo para contagem dos dados extraídos    #
#  do MongoDB usado para armazenagem dos dados     #
#  do Quora                                        #
#                                                  #
####################################################




import pymongo 

uri = ("mongodb://localhost:27017/")
client = pymongo.MongoClient(uri)

db = client.get_database("quora_database")

category_cursor = db.get_collection("category")
category_query_cursor = db.get_collection("category_query")
questions_cursor = db.get_collection("category_query_qid")
answers_cursor = db.get_collection("category_query_aid")
qst_ans_cursor = db.get_collection("question_answer")
topics_cursor = db.get_collection("category_query_tid")
posts_cursor = db.get_collection("category_query_pid")
scp_post_cursor = db.get_collection("space_post")
spaces_cursor = db.get_collection("spaces")

categories = [ c['_id'] for c in category_cursor.find() ]
categories.sort()

queries = {c:[] for c in categories}
for entry in category_query_cursor.find():
    category = entry['category']
    queries[category].append(entry['query'])
for c in categories:
    queries[c].sort()

questions = {query: [] for c in categories for query in queries[c]}
answers   = {query: [] for c in categories for query in queries[c]}
qst_ans   = {e["qid"]: e["aid"] for e in qst_ans_cursor.find()}
topics    = {query: [] for c in categories for query in queries[c]}
posts     = {query: [] for c in categories for query in queries[c]}
spaces    = {query: [] for c in categories for query in queries[c]}
spc_post  = {e["pid"]: e["sid"] for e in scp_post_cursor.find()}

for q in [ entry for entry in questions_cursor.find() ]:
    cat, query = q['category_query'].split("_")
    questions[query].append(q['qid'])
    if q['qid'] in qst_ans.keys():
        answers[query].append(qst_ans[q['qid']])

for t in [ entry for entry in topics_cursor.find() ]:
    cat, query = t['category_query'].split("_")
    topics[query].append(t['tid'])

for p in [ entry for entry in posts_cursor.find() ]:
    cat, query = p['category_query'].split("_")
    posts[query].append(p['pid'])
    spaces[query].append(spc_post[p['pid']])



# Output estruturado
# Eu removi os "&" e "\\" que usei para formatação automática da tabela do latex pra facilitar a visualização
print(f'\n    {"Query":^15s} {"questions":^12s} {"answers":^12s} {"topics":^12s} {"posts":^12s} {"spaces":^12s}')
for cat in categories:
    print(f"/!\\{ cat :-^90s}/!\\")
    for query in queries[cat]:
        # pass
        print(f"    {query:<15s}: \
{len(questions[query]):^12d} \
{len(set(answers[query])):^12d} \
{len(topics[query]):^12d} \
{len(posts[query]):^12d} \
{len(set(spaces[query])):^12d}"\
        )

print(f"/!\\{ 'Total' :-^90s}/!\\")
for c in categories:
    print(f"    {c:<15s}: \
{sum([len(questions[query]) for query in queries[c]]):^12d} \
{sum([len(set(answers[query])) for query in queries[c]]):^12d} \
{sum([len(topics[query]) for query in queries[c]]):^12d} \
{sum([len(posts[query]) for query in queries[c]]):^12d} \
{sum([len(set(spaces[query])) for query in queries[c]]):^12d}"
    )

        