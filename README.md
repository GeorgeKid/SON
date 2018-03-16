# SON
this is a scala implement of SON algorithm

I implement the SON algorithm using A-Priori in order to process each chunk. In each chunk, I count the number of baskets of this chunk, and then get the ratio p = chunk size/ total size, then use the new support = p*s. Then I use the monotonicity of the itemsets to get the all the frequent candidates and pass them to the next count step. In the second step, map process will count support for the candidates in each chunk, and then in the reduce process will add the support for each candidate, and decide to keep this candidate based on s. After filtering, we will have hall the frequent itemsets. 
