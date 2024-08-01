from faker import Faker
from uuid import uuid4
from dataclasses import dataclass, asdict
from random import randint, choice

# Initializing faker module
faker = Faker()

def generate_event_id():
    """
    Generates a unique user ID, product ID, and random event type.

    This function uses `uuid4` to generate unique IDs for the user and product. It then randomly chooses
    between "click", "search", or "purchase" as the event type.

    Returns:
        A tuple containing the user ID, product ID, and event type dictionary (click, search, purchase).
    """
    user_id = str(uuid4())
    product_id = str(uuid4())
    event_type = {
        "click": True, 
        "search": choice([True, False]), 
        "purchase": choice([True, False])
    }
    return user_id, product_id, event_type
        
        

@dataclass
class Ecommerce:
    """
    Represents an e-commerce transaction with user and product details.

    This dataclass defines attributes for user ID, product ID, and additional details that might be generated
    for different event types (click, purchase). You can add more fields as needed.
    """
    user_id: str
    product_id: str
    click_time: str = faker.date()
    click_url: str = faker.url()
    quantity: int = faker.pyint(min_value=1, max_value=10)
    price: str = faker.pyfloat(min_value=1, max_value=1000, right_digits=2) 
    payment_type: str = choice(["credit card", "paypal", "cash"])
    purchase_time: str = faker.date()

    def generate_event_data(self):
        return asdict(self)


@dataclass
class Click: 
    """
    Represents a click event with user ID, product ID, and click time.
    """
    user_id: str
    product_id: str
    click_time: str = faker.time()

@dataclass
class Purchase: 
    """
    Represents a purchase event with user ID, product ID, quantity, price, payment type, and purchase time.
    """
    user_id: str
    product_id: str
    purchase_time: str = faker.time()
    quantity: int = faker.pyint(min_value=1, max_value=10)
    price: str = faker.pyfloat(min_value=1, max_value=1000, right_digits=2) 
    payment_type: str = choice(["credit card", "paypal", "cash"])
    
@dataclass
class Search: 
    """
    Represents a search event with user ID, search time, and search query.
    """
    user_id: str
    search_time: str = faker.time()
    search_query: str = faker.word()




@ dataclass
class Event: 
    """
    Represents an e-commerce event with user ID, product ID, and event type.

    This dataclass defines the core event information. You can extend this by adding methods 
    to generate detailed event data for specific event types like click, purchase, or search.
    """
    user_id: str
    product_id: str
    event_type: str
    
    def gen_click_data(self): 
        if self.event_type["click"]: 
            return asdict(Click(user_id=self.user_id, 
                                product_id=self.product_id))
    
    def gen_search_data(self): 
        if self.event_type["search"]: 
            return asdict(
                Search(user_id=self.user_id, )
            )
    def gen_purchase_data(self): 
        if self.event_type["purchase"]: 
            return asdict(
                Purchase(
                    user_id=self.user_id, 
                    product_id=self.product_id
                )
            )
    def gen_all_event_data(self): 
        click_data = self.gen_click_data()
        purchase_data = self.gen_purchase_data()
        search_data = self.gen_search_data()
        
        return click_data, purchase_data, search_data
    

if __name__ == "__main__": 
    for i in range(2): 
        user_id, product_id, event_type = generate_event_id()
        event = Event(user_id, product_id, event_type)
        print(event.gen_all_event_data())
        
