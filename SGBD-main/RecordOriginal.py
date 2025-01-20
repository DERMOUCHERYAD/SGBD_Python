class Record:
        
    def __init__(self, valeurs):  
        if valeurs is None:
            self.valeurs = []  
        self.valeurs = valeurs  
    
    def __repr__(self):
        return f"Record(valeurs='{self.valeurs})"

    def get_valeurs(self):
        return self.valeurs
    
    def set_valeurs(self,valeurs):
        self.valeurs=valeurs            