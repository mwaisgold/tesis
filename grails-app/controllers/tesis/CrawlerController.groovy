package tesis

import com.mongodb.MongoClient
import com.mongodb.WriteConcern
import com.mongodb.client.MongoCollection
import grails.converters.JSON
import org.bson.Document
import org.grails.web.json.JSONElement

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import static groovyx.gpars.GParsPool.withPool

class CrawlerController {

    static file = "items.json"

    def taskExecutor = Executors.newFixedThreadPool(20)

    private crearAuto(json){
        Automotor.withNewSession {
            def auto = new Automotor(titulo: json.title)
            json.pictures.each {
                auto.addToImagenes(url: it.url)
            }
            json.attributes.each {
                auto.addToAtributos(valor: it.value_name, atributo: it.name)
            }

            auto.save(failOnError: true, flush: true)
        }
    }

    private leer(Collection ids, Closure doRead){
        long init = System.currentTimeMillis()
        def i = new AtomicInteger(0)
        def futures = ids.collect { idAuto ->
            CompletableFuture.runAsync({
                def auto = doRead(idAuto)

                if (i.incrementAndGet() % 100 == 0){
                    println "********** voy ${i.get()} ID: $idAuto"
                    println(auto)

                }
            }, taskExecutor)
        }

        CompletableFuture[] allf = new CompletableFuture[futures.size()]
        futures.eachWithIndex { it, j ->
            allf[j] = it
        }


        CompletableFuture.allOf(allf).thenRun {
            println "************* Duracion ${System.currentTimeMillis() - init}"
        }
    }

    private grabar(Closure action){
        long init = System.currentTimeMillis()
        def i = new AtomicInteger(0)
        def futures = new ArrayList<CompletableFuture>()
        new File(file).withInputStream {
            it.eachLine { line ->
                futures << CompletableFuture.runAsync({
                    if (i.incrementAndGet() % 100 == 0){
                        println "********** voy ${i.get()}"
                    }
                    action(JSON.parse(line))
                }, taskExecutor)
            }
        }

        CompletableFuture[] allf = new CompletableFuture[futures.size()]
        futures.eachWithIndex { it, j ->
            allf[j] = it
        }


        CompletableFuture.allOf(allf).thenRun {
            println "************* Duracion ${System.currentTimeMillis() - init}"
        }
    }

    def grabarEnSql(){
        grabar { JSONElement json -> crearAuto(json) }
        render text: "Fin :)"
    }

    MongoClient mongo

    def grabarEnMongo(){
        def coll = getMongoCollection().withWriteConcern(WriteConcern.JOURNALED)
        grabar { JSONElement json -> coll.insert(mapaDesdeJson(json)) }

        render text: "Fin :)"
    }

    private MongoCollection<Document> getMongoCollection() {
        mongo.getDatabase("tesis").getCollection("automotores")
    }

    def index(String id) {
        JSONElement itemJson = obtenerJson(id)
        def auto = crearAuto(itemJson)
        render auto as JSON
    }

    private static obtenerJson(String id) {
        def text = "https://api.mercadolibre.com/items/$id?attributes=title,pictures,attributes".toURL().getText(readTimeout: 200, requestProperties: [Accept: 'application/json'])
        def itemJson = JSON.parse(text)
        mapaDesdeJson(itemJson)
    }

    private static Map<String, Object> mapaDesdeJson(JSONElement itemJson) {
        [
                title     : itemJson.title,
                pictures  : itemJson.pictures.collect { [url: it.url] },
                attributes: itemJson.attributes.collect { [name: it.name, value_name: it.value_name] }
        ]
    }

    def get(long id){
        render Automotor.get(id) as JSON
    }

    def stressGetDb(){
        def autos = Automotor.list().collect { it.id }
        leer(autos) { idAuto ->
            Automotor.withNewSession {
                Automotor auto = Automotor.get(idAuto)
                (auto as JSON).toString()
            }
        }
    }

    def stressGetMongoDb(){
        def collection = getMongoCollection()
        def autos = collection.find().limit(9902).collect { it.getObjectId("_id") }
        leer(autos) { idAuto ->
            collection.findOne(idAuto).toString()
        }
    }

    static total = 10000
    static pagination = 50

    def crearArchivo(){
        int total = Integer.parseInt(params.total) ?: total
        def itemIds
        withPool(20) {
            itemIds = (0..(total / pagination) - 1).collectParallel { next ->
                def text = "https://api.mercadolibre.com/sites/MLA/search?category=MLA1743&offset=${next * pagination}".toURL().getText(requestProperties: [Accept: 'application/json'])
                def json = JSON.parse(text)
                json.results*.id as Set
            }.flatten()
        }

        Thread.start {
            def items
            def i = new AtomicInteger(0)
            withPool(20) {
                items = itemIds.collectParallel {
                    if (i.incrementAndGet() % 100 == 0)
                        println "************* voy $i"
                    try{
                        obtenerJson(it)
                    } catch (e){
                        null
                    }
                }.findAllParallel {it != null}
            }

            println "***** termine de leer"

            new File("items.json").newOutputStream().withWriter { writer ->
                items.each {
                    writer.println((it as JSON).toString(false))
                }
            }
        }
        render itemIds[0..99] as JSON
    }


}
