import BPTreeNode

class BPTree:
    def __init__(self, order=3):
        self.racine = BPTreeNode(is_leaf=True)  # L'arbre commence avec une feuille vide
        self.order = order    
        
        
    def insert(self, key, value):
        """Insère une clé et sa valeur (rid) dans le B+Tree."""
        racine = self.racine
        
        # Si la racine est pleine alors on doit la scinder; la racine devient un noeud interne
        
        
        if len(racine.keys) == (self.order - 1): # La racine est pleine
            new_racine = BPTreeNode(is_leaf=False) 
            new_racine.children.append(self.racine) 
            self.split_child(new_racine, 0, self.racine) 
            self.racine = new_racine
        self.insert_non_full(self.racine, key, value)

    
    def insert_non_full(self, node, key, value):
        """Insère une clé dans un noeud qui n'est pas plein."""
        if node.is_leaf:
            # Insérer dans une feuille
            i = 0
            while i < len(node.keys) and key > node.keys[i]:
                i += 1
            node.keys.insert(i, key)
            node.children.insert(i, value)
        else:
            # Insérer dans un noeud interne
            i = len(node.keys) - 1
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            if len(node.children[i].keys) == (self.order - 1):
                self.split_child(node, i, node.children[i])
                if key > node.keys[i]:
                    i += 1
            self.insert_non_full(node.children[i], key, value)
    
    
    def split_child(self, parent, index, child):
        """Divise un noeud plein en deux."""
        new_node = BPTreeNode(is_leaf=child.is_leaf)
        mid = len(child.keys) // 2

        # Les clés et enfants de droite vont dans le nouveau noeud
        new_node.keys = child.keys[mid + 1:]
        new_node.children = child.children[mid + 1:]
        child.keys = child.keys[:mid]
        child.children = child.children[:mid + 1]

        # La clé médiane monte dans le parent
        parent.keys.insert(index, child.keys[mid])
        parent.children.insert(index + 1, new_node)

    
    def search(self, key):
        """Recherche une clé dans le B+Tree et renvoie sa valeur."""
        return self._search(self.racine, key)

    
    def _search(self, node, key):
        """Recherche récursive dans le noeud donné."""
        if node.is_leaf:
            if key in node.keys:
                index = node.keys.index(key)
                return node.children[index]
            return None
        else:
            i = 0
            while i < len(node.keys) and key > node.keys[i]:
                i += 1
            return self._search(node.children[i], key)