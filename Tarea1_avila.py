# coding=utf-8
############################### Map & Reduce en Python/dumbo ##############################################################
###########################################################################################################################
#Author: Sergio Arriaga
#Version: v1
#Dataset1: contiene las provincias que componen cada comunidad autónoma
# http://www.campusbigdata.com/tareas/m7_tareas/Comunidades_y_provincias.csv
#Dataset2: Contiene el número de contratos desglosados por provincia, municipio y sexo a lo largo de 2016
# http://www.campusbigdata.com/tareas/m7_tareas/Contratos_por_municipio.csv
#Vamos a tratar de responder a la pregunta,
# ¿Que comunidades autónomas han realizado más contratos a mujeres que a hombres durante todo el periodo?
# Utilizando Map & Reduce en Python/dumbo combinaremos ambos dataseets para responder a dicha pregunta,
# mediante el nexo provincias de ambas tablas
#Corregimos el error en el nombre de Avila en el dataset Contratos_por_municipio.csv

import csv
import sys
from dumbo import main


def load_comunidades(comunidades_files):
    comunidades = {}
    try:
        # Leemos tabla
        with open(comunidades_files) as f:
            reader = csv.reader(f, delimiter=';')
            reader.next()
            for line in reader:
                comunidades[line[1]] = line[0]
    except:
        pass

    return comunidades


class Contratos_Comunidades_Mapper:

    def __init__(self):
        self.comunidades = load_comunidades('./Comunidades_y_provincias.csv')

    def __call__(self, key, value):
        try:
            codigo_mes, provincia, municipio, total_contratos, contratos_hombres, \
            contratos_mujeres = value.split(';')

            int(contratos_hombres)
            int(contratos_mujeres)
            a = self.comunidades[provincia]
            if 'vila' in provincia:  # Corregimos el error en los datos de Avila
                a = 'Castilla y Leon'
            yield a, (contratos_hombres, contratos_mujeres)
        except:
            pass


def join_comunidad_contratos_reduce(key, values):
    total_Hombres = 0
    total_Mujeres = 0
    comunidad = key

    for v in values:
        aux_Hombres, aux_Mujeres = v[:]
        total_Hombres += int(aux_Hombres)
        total_Mujeres += int(aux_Mujeres)
    if total_Mujeres > total_Hombres:
        yield comunidad, (total_Hombres, total_Mujeres)


def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(Contratos_Comunidades_Mapper, join_comunidad_contratos_reduce, opts=inout_opts)


if __name__ == "__main__":
    main(runner)
