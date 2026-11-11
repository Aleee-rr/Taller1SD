"""Pruebas sin conexiones ni escritura en MongoDB."""
import unittest
from app.conflictos import (comparar_vectores, crear_propuesta, resolver_par,
                           unir_vectores, validar_vector)


class TestConflictos(unittest.TestCase):
    def setUp(self):
        self.base = {"n1": 0, "n2": 2, "n3": 2}
        self.a = crear_propuesta(2, "nodo_3.cpu_cores", 1, self.base, 2, 30, "a")
        self.b = crear_propuesta(3, "nodo_3.cpu_cores", 0, self.base, 2, 31, "b")

    def test_iguales(self):
        self.assertEqual(comparar_vectores(self.base, dict(self.base)), "IGUALES")

    def test_a_domina(self):
        self.assertEqual(comparar_vectores({"n1": 0, "n2": 3, "n3": 3}, self.base), "A_DOMINA")

    def test_b_domina(self):
        self.assertEqual(comparar_vectores(self.base, {"n1": 0, "n2": 3, "n3": 3}), "B_DOMINA")

    def test_concurrencia(self):
        self.assertEqual(comparar_vectores(self.a["vector"], self.b["vector"]), "CONCURRENTES")

    def test_mayor_lamport(self):
        r = resolver_par(self.a, self.b)
        self.assertTrue(r["conflicto_detectado"])
        self.assertEqual(r["ganador"]["node"], 3)

    def test_empate_gana_menor_id(self):
        b = dict(self.b, lamport=30)
        self.assertEqual(resolver_par(self.a, b)["ganador"]["node"], 2)

    def test_union(self):
        self.assertEqual(unir_vectores(self.a["vector"], self.b["vector"]), {"n1": 0, "n2": 3, "n3": 3})

    def test_crear_propuesta_no_modifica_original(self):
        self.assertEqual(self.base, {"n1": 0, "n2": 2, "n3": 2})
        self.assertFalse(self.a["confirmed"])

    def test_recurso_distinto_se_rechaza(self):
        b = crear_propuesta(3, "nodo_2.cpu_cores", 0, self.base, 2, 31, "c")
        with self.assertRaisesRegex(ValueError, "mismo recurso"):
            resolver_par(self.a, b)

    def test_base_distinta_se_rechaza(self):
        b = crear_propuesta(3, "nodo_3.cpu_cores", 0, {"n1": 0, "n2": 3, "n3": 2}, 2, 31, "d")
        with self.assertRaisesRegex(ValueError, "mismo snapshot"):
            resolver_par(self.a, b)

    def test_vector_invalido_se_rechaza(self):
        with self.assertRaises(ValueError):
            validar_vector({"1": 0, "2": 2, "3": 2})

    def test_operaciones_duplicadas_se_rechazan(self):
        with self.assertRaisesRegex(ValueError, "diferentes"):
            resolver_par(self.a, self.a)


if __name__ == "__main__":
    unittest.main()
