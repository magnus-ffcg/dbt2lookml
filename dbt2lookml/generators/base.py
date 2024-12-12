
class BaseGenerator():
    """LookML base generator."""
    
    @abstractmethod
    def generate(self, model: DbtModel, **kwargs) -> Dict:
        """Generate LookML."""
        raise NotImplementedError