package tesis

class AutomotorAtributo {

    String valor
    String atributo

    static constraints = {
        valor(nullable: true)

    }

    static belongsTo = Automotor
}
