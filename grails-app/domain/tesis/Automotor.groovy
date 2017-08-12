package tesis

class Automotor {

    String titulo

    static constraints = {

    }

    static hasMany = [imagenes: Imagen, atributos: AutomotorAtributo]

}
