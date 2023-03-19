# DS project hints/suggestions 

## (bozza delle prime idee dalla prima discussione fatta + alcuni consigli del prof)

Example: distributed snapshot, to show a feature of akka
you do not have concurrency, so when you receive a token, in the function that handles the token, you can immediately transmit to all the others and there is no chance that in the meantime you receive money and so you are not recording these correctly. with the actor model you do not have these risk of concurrency

**timeouts**:
when you receive a timeout of a message that you sent to yourself, you have to check if the conditions that you were waiting for still holds
always check if the conditions of the timeouts still holds when receiving the timeout message
because in the meantime maybe the conditions changed and the situation may resolved (before you processed the timeout message)
reference: video lab 3, min 45-46-47


database: a single akka actor

assumption: majority of operations are reads

simulate a crash: either a pre-configured internal timer or a crash message 

do not create 2 different actors for the 2 type of of caches. use the same

clients are in the system since the beginning

system must work in reasonable conditions. 
at some point, maybe nothing work anymore because of the crashes, it is fine

No LRU policies are needed to be implemented



A: timestamp di ultimo update


refill mancati:
invece di last update
associare nella 



sono tornato, datemi i valori
la l1 deve prendere dalle l2 (non senso inverso)


crash l1 
una l2 collgata riceve una richiesta scrittura

richiesta arriva direttamenta al database

l2 si collega direttamente al database 
scrive il database
database non riesce ad aggiornare le altre l2, non ha queste info di topologia
quando l1 torna up manda un recover message alle l2
chiede i loro valori
e chiedendo di cambiare il loro parent in se stessa (dato che è tornata)
ci saranno incosistenze tra loro
contatto il db per queste incostistenze
trovaro valore che avvea scritto diretammente l2 bypassando l1
aggiorno l1 e l2 di consegenza


protocollo che risolve incosistenze


consistency read
(anche da attore esterno volendo)

contatta tutti i nodi
e chiede valori


client at the beginning is given the set of l2 caches

