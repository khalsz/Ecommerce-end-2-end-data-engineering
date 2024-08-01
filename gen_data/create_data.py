from faker import Faker
from uuid import uuid4
from dataclasses import dataclass, asdict
from random import randint, choice

faker = Faker()

def generate_event_id():
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
    user_id: str
    product_id: str
    click_time: str = faker.time()

@dataclass
class Purchase: 
    user_id: str
    product_id: str
    purchase_time: str = faker.time()
    quantity: int = faker.pyint(min_value=1, max_value=10)
    price: str = faker.pyfloat(min_value=1, max_value=1000, right_digits=2) 
    payment_type: str = choice(["credit card", "paypal", "cash"])
    
@dataclass
class Search: 
    user_id: str
    search_time: str = faker.time()
    search_query: str = faker.word()




@ dataclass
class Event: 
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
        
    
# @dataclass
# class Events: 
#     user_id: str
#     event_type: dict
#     product_id: str 
    
    
#     def generate_click_data(self):
#         if not self.event_type["click"]:
#             return 
#         return asdict(Click(
#             user_id=self.user_id,
#             product_id=self.product_id
#         ))
        
#     def generate_search_data(self):
#         if not self.event_type["search"]:
#             return 
#         return asdict(Search(
#             user_id=self.user_id
#         ))
        
#     def generate_purchase_data(self):
#         if not self.event_type["purchase"]:
#             return 
#         return asdict(Purchase(
#             user_id=self.user_id,
#             product_id=self.product_id
#         ))
        
        
#     def generate_all_data(self): 
#         purchase_data = self.generate_purchase_data()
#         click_data = self.generate_click_data()
#         search_data = self.generate_search_data()
#         return purchase_data, click_data, search_data


# @dataclass
# class Search:
#     user_id: str 
#     search_time: str = faker.date()
#     search_query: str = faker.word()
    
# @dataclass
# class Purchase:
#     user_id: str 
#     product_id: str 
#     time_stamp: str = faker.date()
#     quantity: int = faker.pyint(min_value=1, max_value=10)
#     price: str = faker.pyfloat(min_value=1, max_value=1000, right_digits=2) 
#     payment_type: str = choice(["credit card", "paypal", "cash"])


# if __name__ == "__main__":
#     # print(generate_event_data())
#     for i in range(2):
#         user_id, product_id = generate_event_id()
#         event = Ecommerce(user_id,  product_id)
#         # print(event.generate_event_data())




# from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# schema =   StructType([StructField("user_id", StringType()), 
#                        StructField("product_id", StringType()), 
#                        StructField("time_stamp", StringType()), 
#                        StructField("quantity", IntegerType()), 
#                        pr])







# from faker import Faker
# from uuid import uuid4
# from dataclasses import dataclass, asdict
# from random import randint, choice

# faker = Faker()

# def generate_event_data():
#     user_id = str(uuid4())
#     product_id = str(uuid4())
#     event_type = {
#         "click": choice([True]),
#         "search": choice([True, False]),
#         "purchase": choice([True, False])
#     }
#     return event_type, user_id, product_id
        
        

# @dataclass
# class Click:
#     user_id: str 
#     product_id: str 
#     click_time: str = faker.date()

# @dataclass
# class Search:
#     user_id: str 
#     search_time: str = faker.date()
#     search_query: str = faker.word()
    
# @dataclass
# class Purchase:
#     user_id: str 
#     product_id: str 
#     time_stamp: str = faker.date()
#     quantity: int = faker.pyint(min_value=1, max_value=10)
#     price: str = faker.pyfloat(min_value=1, max_value=1000, right_digits=2) 
#     payment_type: str = choice(["credit card", "paypal", "cash"])


# @dataclass
# class Events: 
#     user_id: str
#     event_type: dict
#     product_id: str 
    
    
#     def generate_click_data(self):
#         if not self.event_type["click"]:
#             return 
#         return asdict(Click(
#             user_id=self.user_id,
#             product_id=self.product_id
#         ))
        
#     def generate_search_data(self):
#         if not self.event_type["search"]:
#             return 
#         return asdict(Search(
#             user_id=self.user_id
#         ))
        
#     def generate_purchase_data(self):
#         if not self.event_type["purchase"]:
#             return 
#         return asdict(Purchase(
#             user_id=self.user_id,
#             product_id=self.product_id
#         ))
        
        
#     def generate_all_data(self): 
#         purchase_data = self.generate_purchase_data()
#         click_data = self.generate_click_data()
#         search_data = self.generate_search_data()
#         return purchase_data, click_data, search_data




# for i in range(2):
#     event_type, user_id, product_id = generate_event_data()
#     event = Events(user_id, event_type, product_id)
#     print(event.generate_all_data())




# from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# schema =   StructType([StructField("user_id", StringType()), 
#                        StructField("product_id", StringType()), 
#                        StructField("time_stamp", StringType()), 
#                        StructField("quantity", IntegerType()), 
#                        pr])