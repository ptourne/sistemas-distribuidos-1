#import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)s
from transformers import pipeline
import ast

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 100)

##credits_df = pd.read_csv('../datasets/credits.csv')
movies_df = pd.read_csv('../client/datasets/movies_metadata.csv')
ratings_df = pd.read_csv('../client/datasets/ratings.csv')
ratings_df = ratings_df.head(6000000)
movies_df_columns = ["id", "title", "genres", "release_date", "overview", "production_countries", "spoken_languages", "budget", "revenue"]
credits_df_columns = ["id", "cast"]
ratings_df_columns = ["movieId", "rating"]
#print("credits_df shape:", credits_df.shape)
movies_df_cleaned = movies_df.dropna(subset=movies_df_columns)[movies_df_columns].copy()
ratings_df_cleaned = ratings_df.dropna(subset=ratings_df_columns)[ratings_df_columns].copy()
print(f"ratings_df_cleaned shape: {ratings_df_cleaned.shape}")
#credits_df_cleaned = credits_df.dropna(subset=credits_df_columns)[credits_df_columns].copy()
#print(f"credits_df_cleaned shape: {credits_df_cleaned.shape}")

movies_df_cleaned['release_date'] = pd.to_datetime(movies_df_cleaned['release_date'], format='%Y-%m-%d', errors='coerce')
movies_df_cleaned['budget'] = pd.to_numeric(movies_df_cleaned['budget'], errors='coerce')
movies_df_cleaned['revenue'] = pd.to_numeric(movies_df_cleaned['revenue'], errors='coerce')
# Replace json fields with string arrays
def dictionary_to_list(dictionary_str):
    try:
        dictionary_list = ast.literal_eval(dictionary_str)  
        return [data['name'] for data in dictionary_list]  
    except (ValueError, SyntaxError):
        return [] 

# credits_df_cleaned['cast'] = credits_df_cleaned['cast'].apply(dictionary_to_list)
# non_empty_cast_count = credits_df_cleaned[credits_df_cleaned['cast'].apply(lambda x: len(x) > 0)].shape[0]

movies_df_cleaned['genres'] = movies_df_cleaned['genres'].apply(dictionary_to_list)
movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].apply(dictionary_to_list)
movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].apply(dictionary_to_list)
movies_df_cleaned['genres'] = movies_df_cleaned['genres'].astype(str)
movies_df_cleaned['production_countries'] = movies_df_cleaned['production_countries'].astype(str)
movies_df_cleaned['spoken_languages'] = movies_df_cleaned['spoken_languages'].astype(str)

print(f"movies cleaned: {movies_df_cleaned.shape}")

# solo_country_df = movies_df_cleaned[movies_df_cleaned['production_countries'].apply(lambda x: len(eval(x)) == 1)]
# solo_country_df.loc[:, 'country'] = solo_country_df['production_countries'].apply(lambda x: eval(x)[0])
# cant_by_country = solo_country_df.groupby('country').size()

# # Ver cuántas son de India
# cantidad_india = cant_by_country.get('India', 0)

#print(f"Cantidad de películas producidas solo en India: {cantidad_india}")

movies_argentina_post_2000_df = movies_df_cleaned[
    (movies_df_cleaned['production_countries'].str.contains('Argentina', case=False, na=False)) & 
    (movies_df_cleaned['release_date'].dt.year >= 2000)
]
print(f"movies arg: {movies_argentina_post_2000_df.shape}")

# # count credits_df_cleaned when cast != []

movies_argentina_post_2000_df["id"] = movies_argentina_post_2000_df["id"].astype(str)
# credits_df_cleaned["id"] = credits_df_cleaned["id"].astype(str)
# cast_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(credits_df_cleaned,
#                                                                                 on="id")
# print(f"cast_arg_post_2000_df: {cast_arg_post_2000_df.shape}")
# total_credits = 0

# res_obtenidos = {"101006":3,"104431":4,"10817":4,"109690":11,"110393":19,"111188":47,"11148":12,"114285":19,"115173":3,"11556":5,"118204":5,"11928":24,"124198":5,"125619":15,"126104":9,"126883":6,"127257":7,"127304":7,"127326":6,"127445":9,"127880":5,"128062":8,"128070":3,"128237":9,"128598":10,"129115":5,"129933":3,"133082":5,"13317":7,"133786":10,"134096":4,"13653":17,"138167":4,"153158":15,"16":29,"16092":7,"161024":7,"16425":6,"1653":13,"16916":10,"18079":17,"188761":8,"19460":21,"1956":2,"196324":3,"199851":5,"20316":4,"204372":13,"206292":4,"21043":8,"21409":4,"214758":7,"214761":17,"222759":6,"222772":11,"25376":31,"254719":10,"25532":19,"257487":5,"259843":4,"262788":8,"265195":15,"265563":4,"266031":10,"267579":8,"268297":4,"26864":11,"270909":5,"27092":5,"273261":7,"278458":5,"284870":10,"285463":8,"28710":17,"288312":15,"289180":4,"29275":5,"296778":9,"297641":9,"297677":3,"29952":10,"30125":11,"311215":8,"323214":3,"324578":17,"33107":12,"336664":8,"336808":7,"337115":8,"341744":7,"35005":5,"35026":28,"351067":6,"351454":19,"351809":12,"352161":8,"353989":8,"35837":4,"358505":8,"359641":12,"359736":1,"360365":15,"361251":5,"36241":5,"365002":9,"36971":5,"374764":4,"382906":6,"389578":4,"389995":6,"391617":3,"39378":9,"394374":6,"398830":1,"399615":5,"405446":3,"408537":7,"41272":9,"413666":9,"416680":1,"418718":10,"41924":14,"420648":8,"425942":10,"428863":8,"43432":15,"44282":6,"44413":5,"444935":7,"44989":5,"45722":4,"47065":10,"47261":9,"47383":10,"49073":11,"49681":9,"50724":7,"51548":4,"57019":10,"57489":5,"58429":6,"62342":5,"64462":5,"65795":4,"6636":6,"67884":10,"68047":5,"69278":5,"69985":26,"71114":3,"71118":4,"71576":4,"7274":5,"73293":6,"73889":4,"75193":2,"76696":9,"78115":6,"78237":16,"78570":4,"78705":5,"79767":8,"79983":13,"80437":8,"80717":10,"81022":2,"82999":9,"83266":5,"83505":5,"84302":3,"85543":5,"85544":4,"85549":5,"87017":10,"87380":15,"8898":6,"8900":9,"92780":5,"94649":5,"99648":3,"99896":6}

# for i, row in cast_arg_post_2000_df.iterrows():    
#     cant = 0
#     cast = row['cast']
#     total_credits += len(cast)
#     #print(f"id: {row['id']}, cant: {len(cast)}")
#     obtenido = res_obtenidos.get(row['id'], 0)
#     if len(cast) != obtenido:
#         print(f"Error: {row['id']}, cant: {len(cast)}, obtenido: {obtenido}")
# print(f"total_credits: {total_credits}")
ratings_df_cleaned["movieId"] = ratings_df_cleaned["movieId"].astype(str)
ranking_arg_post_2000_df = movies_argentina_post_2000_df[["id", "title"]].merge(ratings_df_cleaned,
                                                                                 left_on="id",
                                                                                 right_on="movieId")
mean_ranking_arg_post_2000_df = ranking_arg_post_2000_df.groupby(["id", "title"])['rating'].mean().reset_index()
print(f"mean_ranking_arg_post_2000_df: {mean_ranking_arg_post_2000_df.shape}")

# for i, row in mean_ranking_arg_post_2000_df.iterrows():    
#     print(f"id: {row['id']}, avg: {row['rating']}")
    
print(mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmax()])
print(mean_ranking_arg_post_2000_df.iloc[mean_ranking_arg_post_2000_df['rating'].idxmin()])

# Q5

# q5_input_df = movies_df_cleaned.copy()
# print(f"q5_input_df shape: {q5_input_df.shape}") # (5370, 9)
# q5_input_df = q5_input_df.loc[q5_input_df['budget'] != 0]
# q5_input_df = q5_input_df.loc[q5_input_df['revenue'] != 0]
# print(f"q5_input_df shape: {q5_input_df.shape}") # (5370, 9)
# sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
# q5_input_df['sentiment'] = q5_input_df['overview'].fillna('').apply(
#     lambda x: sentiment_analyzer(x, truncation=True)[0]['label']
# )
# q5_input_df["rate_revenue_budget"] = q5_input_df["revenue"] / q5_input_df["budget"]
#print(q5_input_df.sample(10))    
#print("cant by sentiment: ", q5_input_df['sentiment'].value_counts())
# cant by sentiment:  
# POSITIVE    3091
# NEGATIVE    2279
# average_rate_by_sentiment = q5_input_df.groupby("sentiment")["rate_revenue_budget"].mean()
# print(average_rate_by_sentiment)                                                                   

# NEGATIVE    5453.397595
# POSITIVE    5668.650541
